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
"""Check that every non-Apache-2.0 dependency in a shaded jar ships its license text.

A dependency is bundled when every class it declares is in the shaded jar.
Its license comes from its own pom, or from the license file it ships when the
pom chain declares none. Anything not Apache-2.0 must have
META-INF/licenses/LICENSE-<artifactId>, and a text with no dependency behind it
is reported too.

Flow, for each */target/*-bundled.jar:

  main()            find the jars, print each report, exit non-zero on any failure
  check()           everything below, for one jar
    tree_coords()     exact coordinates Maven resolved for the module
    shade_includes()  which of those the shade plugin bundles
    class_entries()   a dependency is bundled when every class it declares is
                      in the jar; native and aggregator jars declare none
    license_names()   its license, from its pom, walking the parent chain
    jar_license()     or from the license file it ships, when the pom is silent
    is_apache_2()     Apache-2.0 needs no text, the banner already covers it
    text_disagrees()  otherwise LICENSE-<artifactId> must exist and must match
                      the declared license, via identify()

Requires <module>/target/dependency-tree.txt, written by:

  ./mvnw -DskipTests dependency:tree -DoutputType=text -DoutputFile=target/dependency-tree.txt

Usage:  validate_bundled_license_texts.py [<jar> ...]
"""
from __future__ import annotations

import pathlib
import re
import sys
import xml.etree.ElementTree as ET
import zipfile
from functools import lru_cache

M2 = pathlib.Path.home() / ".m2" / "repository"
ROOT = pathlib.Path(__file__).resolve().parents[2]
TREE = "target/dependency-tree.txt"

# Shade cannot merge module descriptors, so it drops them from every dependency.
DROPPED_BY_SHADE = {"module-info.class"}


def tree_coords(tree: pathlib.Path) -> list[tuple[str, str, str, str]]:
    """(group, artifact, version, classifier) for every dependency Maven resolves.

    Maven prints group:artifact:type[:classifier]:version:scope, so the version
    is the second-to-last field. The tree must not be scope-filtered:
    -Dscope=runtime drops protobuf-java, which is bundled, because its path runs
    through a provided-scoped node.
    """
    coords = []
    for line in tree.read_text().splitlines():
        parts = re.sub(r"^[| +\\-]+", "", line).split(":")
        if len(parts) < 5 or parts[2] in ("pom", "test-jar"):
            continue
        coords.append((parts[0], parts[1], parts[-2], parts[3] if len(parts) >= 6 else ""))
    return coords


def shade_includes(pom: pathlib.Path) -> set[str]:
    """The groupId:artifactId entries maven-shade-plugin is told to bundle."""
    block = re.search(r"<artifactSet>(.*?)</artifactSet>", pom.read_text(), re.DOTALL)
    if block is None:
        return set()
    scala = re.search(r"<scala\.binary\.version>([^<]+)<", (ROOT / "pom.xml").read_text())
    return {
        include.replace("${scala.binary.version}", scala.group(1) if scala else "")
        for include in re.findall(r"<include>([^<]+)</include>", block.group(1))
    }


def dependency_jar(group: str, artifact: str, version: str, classifier: str) -> pathlib.Path:
    """Where a resolved coordinate lives in the local repository."""
    name = f"{artifact}-{version}-{classifier}.jar" if classifier else f"{artifact}-{version}.jar"
    return M2 / pathlib.Path(group.replace(".", "/")) / artifact / version / name


def class_entries(jar: pathlib.Path) -> set[str]:
    """The classes a dependency declares, less what shade cannot carry over."""
    with zipfile.ZipFile(jar) as archive:
        return {
            entry
            for entry in archive.namelist()
            if entry.endswith(".class") and entry.rsplit("/", 1)[-1] not in DROPPED_BY_SHADE
        }


def _tag(xml: str, tag: str) -> str:
    """The text of an element, by regex, for poms ElementTree will not parse."""
    match = re.search(rf"<{tag}>(.*?)</{tag}>", xml, re.DOTALL)
    return match.group(1).strip() if match else ""


def _children(element: ET.Element, name: str) -> list[ET.Element]:
    """Direct children with this name, ignoring how the pom binds its namespace."""
    return [child for child in element if child.tag.rsplit("}", 1)[-1] == name]


def _text(element: ET.Element, name: str) -> str:
    """The text of the first child with this name."""
    for child in _children(element, name):
        return (child.text or "").strip()
    return ""


@lru_cache(maxsize=None)
def license_names(group: str, artifact: str, version: str) -> tuple[str, ...]:
    """Declared license names, walking up the parent chain when a pom declares none."""
    pom = M2 / pathlib.Path(group.replace(".", "/")) / artifact / version / f"{artifact}-{version}.pom"
    if not pom.exists():
        return ()
    try:
        root = ET.parse(pom).getroot()
    except ET.ParseError:
        # hadoop-project and a few others are valid to Maven but not to expat.
        text = pom.read_text(errors="replace")
        block = re.search(r"<licenses>(.*?)</licenses>", text, re.DOTALL)
        names = tuple(n.strip() for n in re.findall(r"<name>(.*?)</name>", block.group(1), re.DOTALL) if n.strip()) if block else ()
        if names:
            return names
        parent = re.search(r"<parent>(.*?)</parent>", text, re.DOTALL)
        coord = tuple(_tag(parent.group(1), t) for t in ("groupId", "artifactId", "version")) if parent else ()
        return license_names(*coord) if coord and all(coord) and "${" not in "".join(coord) else ()

    names = tuple(
        name
        for block in _children(root, "licenses")
        for entry in _children(block, "license")
        for name in [_text(entry, "name")]
        if name
    )
    if names:
        return names
    for parent in _children(root, "parent"):
        coord = (_text(parent, "groupId"), _text(parent, "artifactId"), _text(parent, "version"))
        if all(coord) and "${" not in "".join(coord):
            return license_names(*coord)
    return ()


LICENSE_FILE = re.compile(r"(META-INF/)?licen[sc]e([-_.].*)?", re.I)
APACHE_TEXT = re.compile(r"Apache License\s+Version 2\.0|Licensed under the Apache License, Version 2\.0", re.I)


def jar_license(jar: pathlib.Path) -> str | None:
    """The license an artifact ships, for poms that declare none."""
    with zipfile.ZipFile(jar) as archive:
        for name in sorted(archive.namelist()):
            if not LICENSE_FILE.fullmatch(name):
                continue
            text = archive.read(name).decode("utf-8", "replace").strip()
            if text:
                return "Apache License 2.0" if APACHE_TEXT.search(text) else text.splitlines()[0].strip()
    return None


def is_apache_2(names: tuple[str, ...]) -> bool:
    """Whether any declared name is Apache-2.0, which needs no per-artifact text."""
    return any(re.search(r"apache", n, re.I) and re.search(r"2(\.0)?\b", n) for n in names)


# Ordered most specific first; the first match wins.
FAMILIES = (
    ("MPL", r"mozilla public license"),
    ("CDDL", r"common development and distribution license|\bCDDL\b"),
    ("EPL", r"eclipse public license"),
    ("EDL", r"eclipse distribution license"),
    ("CPL", r"common public license"),
    ("Apache", r"apache (software )?license|\bapache[- ]2"),
    ("MIT", r"\bMIT\b|permission is hereby granted, free\s+of charge"),
    ("BSD", r"\bBSD\b|redistribution and use in source and binary forms"),
    ("GPL", r"gnu (general|lesser) public license"),
    ("Public Domain", r"public domain"),
)


def identify(text: str) -> tuple[str | None, str | None]:
    """(family, version) named by a license name or the head of a license text."""
    head = "\n".join(text.splitlines()[:40])
    for family, pattern in FAMILIES:
        match = re.search(pattern, head, re.I)
        if not match:
            continue
        # A version only counts when it follows the name, and must be dotted so
        # that the 2 in "CDDL + GPLv2" is not read as CDDL's version.
        after = head[match.end() : match.end() + 30]
        version = re.search(r"(?:version\s*|v\s*|-\s*)?(\d+\.\d+)", after, re.I)
        return family, version.group(1) if version else None
    return None, None


# Committed texts that do not match the declared license and cannot be corrected
# from anything the artifact carries: jamon-runtime declares MPL 1.1 and ships
# MPL 2.0 text, javolution 5.5.1 declares BSD at a dead URL and ships the
# project's later MIT text.
KNOWN_TEXT_MISMATCHES = {"jamon-runtime", "javolution"}


def text_disagrees(declared: str, text: str) -> str | None:
    """Why a committed license text does not match the license declared for it."""
    if not text.strip():
        return "the committed text is empty"
    want_family, want_version = identify(declared)
    got_family, got_version = identify(text)
    if want_family and got_family and want_family != got_family:
        return f"declared {want_family}, text is {got_family}"
    if want_version and got_version and want_version != got_version:
        return f"declared {want_family} {want_version}, text is {got_family} {got_version}"
    return None


def check(module: pathlib.Path, jar_path: pathlib.Path) -> dict[str, list]:
    """Every way one bundled jar's license texts can be wrong, keyed by kind."""
    tree = module / TREE
    if not tree.exists():
        raise SystemExit(f"FAIL {module.name}: {TREE} is missing; see this script's usage.")

    with zipfile.ZipFile(jar_path) as jar:
        names = jar.namelist()
    bundled_classes = {entry for entry in names if entry.endswith(".class")}
    # Only the texts this project curates; a dependency may ship its own
    # META-INF/licenses/ directory, which is attribution to keep.
    texts = {entry.split("/")[-1] for entry in names if entry.startswith("META-INF/licenses/LICENSE-")}
    includes = shade_includes(module / "pom.xml")

    result: dict[str, list] = {"missing": [], "mismatched": [], "unresolved": [], "partial": []}
    bundled = []
    for group, artifact, version, classifier in tree_coords(tree):
        if group == "org.apache.xtable" or f"{group}:{artifact}" not in includes:
            continue
        coord = f"{group}:{artifact}:{version}"
        jar = dependency_jar(group, artifact, version, classifier)
        if not jar.exists():
            result["unresolved"].append(coord)
            continue
        # A dependency in both the tree and <includes> is bundled. Classes are
        # how that gets verified; native-binary and aggregator jars have none,
        # so they are taken at their word rather than skipped.
        declared_classes = class_entries(jar)
        if declared_classes:
            present = declared_classes & bundled_classes
            if not present:
                continue
            if present != declared_classes:
                result["partial"].append(f"{coord}  ({len(present)}/{len(declared_classes)} classes)")
                continue

        licenses = license_names(group, artifact, version) or tuple(filter(None, [jar_license(jar)]))
        if not licenses:
            result["unresolved"].append(f"{coord}  (no license declared or shipped)")
            continue
        if is_apache_2(licenses):
            continue
        bundled.append(artifact)
        if f"LICENSE-{artifact}" not in texts:
            result["missing"].append(f"{coord}  ({licenses[0]})")
            continue
        with zipfile.ZipFile(jar_path) as shaded:
            committed = shaded.read(f"META-INF/licenses/LICENSE-{artifact}").decode("utf-8", "replace")
        reason = text_disagrees(licenses[0], committed)
        if reason and artifact not in KNOWN_TEXT_MISMATCHES:
            result["mismatched"].append(f"{coord}  ({reason})")

    result["orphaned"] = sorted(texts - {f"LICENSE-{artifact}" for artifact in bundled})
    result["count"] = len(bundled)
    return result


REPORTS = [
    ("missing", "bundled non-Apache-2.0 dependencies with no license text"),
    ("mismatched", "license texts that do not match the declared license"),
    ("orphaned", "license texts with no bundled non-Apache-2.0 dependency"),
    ("partial", "dependencies only partly present, so they cannot be classified"),
    ("unresolved", "dependencies that could not be resolved or have no license"),
]


def main() -> None:
    """Check every bundled jar, or only those named on the command line."""
    jars = [pathlib.Path(a) for a in sys.argv[1:]] or sorted(ROOT.glob("**/target/*-bundled.jar"))
    if not jars:
        raise SystemExit(
            "FAIL: no */target/*-bundled.jar found; nothing was validated.\n"
            "Build the shaded modules first:  ./mvnw -DskipTests package"
        )

    status = 0
    for jar_path in jars:
        result = check(jar_path.parent.parent, jar_path)
        failed = False
        for key, description in REPORTS:
            if result[key]:
                status, failed = 1, True
                print(f"FAIL {jar_path.name}: {description}:")
                for entry in sorted(result[key]):
                    print(f"  - {entry}")
        if not failed:
            print(f"OK   {jar_path.name}: {result['count']} bundled non-Apache-2.0 dependencies, all with license texts.")

    raise SystemExit(status)


if __name__ == "__main__":
    main()
