#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Check that every bundled non-Apache-2.0 dependency ships its license text.

This reads the built ``*-bundled.jar`` rather than the poms. The published
artifact is the only thing that settles what is actually bundled: a shade
``<includes>`` list can name dependencies that no longer resolve, and a module
with ``<filters>`` or an uber-jar dependency ships classes the dependency tree
does not describe. It is also the artifact a release reviewer inspects.

For each bundled jar, two directions are checked against the coordinate listing
the jar carries in ``META-INF/LICENSE``:

  missing   a non-Apache-2.0 dependency whose classes are in the jar, with no
            ``META-INF/licenses/LICENSE-<artifactId>``
  orphaned  a ``META-INF/licenses/`` text with no such dependency behind it

Usage:  validate_bundled_license_texts.py [<jar> ...]
With no arguments, every ``*/target/*-bundled.jar`` in the repo is checked, and
the run fails if none were found -- an empty check must never report success.
"""
from __future__ import annotations

import pathlib
import re
import sys
import zipfile

APACHE_FAMILY = "Apache License 2.0"
M2 = pathlib.Path.home() / ".m2" / "repository"
ROOT = pathlib.Path(__file__).resolve().parents[2]

# A dependency's own classes are sampled and looked for in the bundle. Shade
# drops unreferenced classes in modules that configure <filters>, so a bundled
# dependency does not contribute all of its classes -- but it always
# contributes far more than none.
SAMPLE = 80
PRESENT_RATIO = 4


def parse_bundled_listing(text: str) -> dict[str, list[tuple[str, str, str]]]:
    """family -> coordinates, from the LICENSE-bundled layout.

    A family is a line immediately followed by a dashed underline; coordinates
    are the ``group:artifact:version`` lines beneath it.
    """
    groups: dict[str, list[tuple[str, str, str]]] = {}
    family = None
    lines = text.splitlines()
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            family = None
        elif index + 1 < len(lines) and re.fullmatch(r"-{3,}", lines[index + 1].strip()):
            family = stripped
        elif re.fullmatch(r"-{3,}", stripped):
            continue
        elif family and stripped.count(":") == 2:
            groups.setdefault(family, []).append(tuple(stripped.split(":")))
    return groups


def _classes_in(path: pathlib.Path) -> list[str]:
    with zipfile.ZipFile(path) as dependency:
        return [entry for entry in dependency.namelist() if entry.endswith(".class")]


def dependency_classes(group: str, artifact: str, version: str) -> list[str] | None:
    """Class entries of the dependency's own jar, or None if it cannot be found.

    The version in the listing is not trusted to be the one that resolves. Where
    it has gone stale -- the listing says jersey 1.9 and 1.19 is what builds --
    only the resolved version is ever downloaded, so keying strictly on the
    listed version finds nothing in ~/.m2 and the dependency reads as absent
    from the jar. Its committed text then looks like an orphan and the check
    fails for a dependency that is genuinely bundled.

    Any version of the same groupId:artifactId answers the question actually
    being asked, which is whether this artifact's classes are in the bundle.
    Whether the listed version is right is a separate accuracy problem, and one
    this check is not able to settle.
    """
    home = M2 / pathlib.Path(group.replace(".", "/")) / artifact
    exact = home / version
    for directory in [exact, *(d for d in sorted(home.glob("*")) if d.is_dir() and d != exact)]:
        for path in sorted(directory.glob(f"{artifact}-*.jar")):
            if path.name.endswith(("-sources.jar", "-javadoc.jar")):
                continue
            classes = _classes_in(path)
            if classes:
                return classes[:SAMPLE]
    return None


def check(jar_path: pathlib.Path) -> tuple[list, list, int]:
    with zipfile.ZipFile(jar_path) as jar:
        names = jar.namelist()
        listing_entry = "META-INF/LICENSE" if "META-INF/LICENSE" in names else "META-INF/LICENSE-bundled"
        if listing_entry not in names:
            raise SystemExit(f"FAIL {jar_path.name}: no META-INF/LICENSE to validate against")
        listing = jar.read(listing_entry).decode("utf-8")
    bundled_classes = {entry for entry in names if entry.endswith(".class")}
    # Only the texts this project curates, which are named LICENSE-<artifactId>.
    # A dependency may ship its own META-INF/licenses/ directory -- groovy-all
    # carries antlr2-license.txt, asm-license.txt and three more, and its own
    # META-INF/LICENSE refers to them ("See licenses/antlr2-license.txt for
    # details") for code compiled into it. Those are attribution the bundle must
    # keep, not orphans to delete, so they are not considered here.
    texts = {
        entry.split("/")[-1]
        for entry in names
        if entry.startswith("META-INF/licenses/LICENSE-") and not entry.endswith("/")
    }

    missing, in_jar_non_apache = [], []
    for family, coords in parse_bundled_listing(listing).items():
        if family == APACHE_FAMILY:
            continue
        for group, artifact, version in coords:
            sample = dependency_classes(group, artifact, version)
            if sample is None:
                continue  # unresolvable, so nothing of it can be in the jar
            if sum(entry in bundled_classes for entry in sample) < max(1, len(sample) // PRESENT_RATIO):
                continue  # listed, but its classes are not here
            in_jar_non_apache.append((group, artifact, version, family))
            if f"LICENSE-{artifact}" not in texts:
                missing.append((group, artifact, version, family))

    expected = {f"LICENSE-{artifact}" for _, artifact, _, _ in in_jar_non_apache}
    orphaned = sorted(texts - expected)
    return missing, orphaned, len(in_jar_non_apache)


def main() -> None:
    if len(sys.argv) > 1:
        jars = [pathlib.Path(argument) for argument in sys.argv[1:]]
    else:
        jars = sorted(ROOT.glob("**/target/*-bundled.jar"))
        if not jars:
            raise SystemExit(
                "FAIL: no */target/*-bundled.jar found; nothing was validated.\n"
                "Build the shaded modules first:  ./mvnw -DskipTests package"
            )

    status = 0
    for jar_path in jars:
        missing, orphaned, checked = check(jar_path)
        if missing:
            status = 1
            print(f"FAIL {jar_path.name}: bundled non-Apache-2.0 dependencies with no META-INF/licenses/ text:")
            for group, artifact, version, family in sorted(missing):
                print(f"  - LICENSE-{artifact}  ({group}:{artifact}:{version}, {family})")
        if orphaned:
            status = 1
            print(f"FAIL {jar_path.name}: META-INF/licenses/ texts with no bundled non-Apache-2.0 dependency:")
            for name in orphaned:
                print(f"  - {name}")
        if not missing and not orphaned:
            print(f"OK   {jar_path.name}: {checked} bundled non-Apache-2.0 dependencies, all with license texts.")

    raise SystemExit(status)


if __name__ == "__main__":
    main()
