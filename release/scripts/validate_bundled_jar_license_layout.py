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
"""Check the license layout of the jars we publish.

validate_shaded_license_coverage.sh checks the source tree. This checks the
built artifact, which is what a release reviewer actually inspects and where
the problems in https://github.com/apache/incubator-xtable/issues/701 were
found: jars carrying the same license text twice under two names, and bundles
carrying two separate directories of license texts.

Every jar must have exactly one canonical META-INF/LICENSE and NOTICE, no
LICENSE-bundled or NOTICE-bundled beside them, and at most one directory of
license texts, at META-INF/licenses.

Usage:  validate_bundled_jar_license_layout.py [<jar> ...]
With no arguments every */target/*.jar in the repo is checked. Finding no jars
is a failure -- an empty run must not report success.
"""
from __future__ import annotations

import pathlib
import sys
import zipfile

ROOT = pathlib.Path(__file__).resolve().parents[2]

CANONICAL = ("META-INF/LICENSE", "META-INF/NOTICE")
# Inputs to the shade transformers. They belong in target/classes, never in a jar.
NOT_PACKAGED = ("META-INF/LICENSE-bundled", "META-INF/NOTICE-bundled")
LICENSE_DIR = "META-INF/licenses/"


def find_jars() -> list[pathlib.Path]:
    skip = ("-sources.jar", "-javadoc.jar", "-tests.jar")
    # Globbed per module depth rather than with **, which would walk node_modules.
    found = set(ROOT.glob("*/target/*.jar")) | set(ROOT.glob("*/*/target/*.jar"))
    return sorted(
        jar for jar in found if not jar.name.endswith(skip) and "original-" not in jar.name
    )


def check(jar: pathlib.Path) -> list[str]:
    with zipfile.ZipFile(jar) as archive:
        entries = set(archive.namelist())
        interesting = set(CANONICAL) | set(NOT_PACKAGED)
        contents = {name: archive.read(name) for name in entries & interesting}
    problems = []

    for name in CANONICAL:
        if name not in entries:
            problems.append(f"missing {name}")

    for name in NOT_PACKAGED:
        if name in entries:
            canonical = name.replace("-bundled", "")
            same = contents.get(canonical) == contents[name]
            detail = f" (byte-identical to {canonical})" if same else ""
            problems.append(f"{name} should not be packaged{detail}")

    # Any other directory of license texts, e.g. the META-INF/license netty ships.
    # Matched on a path segment being exactly license/licenses, so that classes
    # like org/apache/ivy/core/module/descriptor/License.class are not mistaken
    # for one. Classes never carry license text, so they are out of scope anyway.
    other_dirs = set()
    for name in entries:
        if name.endswith((".class", "/")) or name.startswith(LICENSE_DIR):
            continue
        parts = name.split("/")[:-1]
        if any(part.lower() in ("license", "licenses") for part in parts):
            other_dirs.add("/".join(parts) + "/")
    other_dirs = sorted(other_dirs)
    for directory in other_dirs:
        problems.append(f"license texts outside {LICENSE_DIR}: {directory}")

    return problems


def label_for(jar: pathlib.Path) -> str:
    try:
        return str(jar.resolve().relative_to(ROOT))
    except ValueError:
        return str(jar)


def main() -> int:
    jars = [pathlib.Path(a) for a in sys.argv[1:]] or find_jars()
    if not jars:
        print("FAIL no jars found to check; build the modules first.")
        return 1

    status = 0
    for jar in jars:
        problems = check(jar)
        label = label_for(jar)
        if problems:
            status = 1
            print(f"FAIL {label}:")
            for problem in problems:
                print(f"  - {problem}")
        else:
            print(f"OK   {label}: one canonical LICENSE and NOTICE, one licenses directory.")
    return status


if __name__ == "__main__":
    sys.exit(main())
