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
"""Check that the license validators fail when they should.

The License Check once passed for months because a missing tool made it check
nothing. Each test here plants one fault in a copy of the repo and asserts the
validator exits non-zero, so a check that goes vacuous fails CI instead.

Run after the steps in mvn-license-check.yml, which build what this needs:
the shaded bundles and their dependency trees. Nothing is skipped when they are
missing; that is a failure like any other.

Usage:  test_license_checks.py
"""
from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest
import zipfile

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPTS = "release/scripts"
EXTENSIONS = "xtable-hudi-support/xtable-hudi-support-extensions"
UTILS_JAR_GLOB = "xtable-hudi-support/xtable-hudi-support-utils/target/*.jar"
# Tools the coverage script uses, linked into a bare PATH for the missing-tool test.
SHELL_TOOLS = ("bash", "git", "grep", "sed", "awk", "sort", "comm", "diff", "mktemp",
               "dirname", "rm", "cat", "basename", "printf", "xargs", "head")


def build_outputs() -> list[pathlib.Path]:
    """What the validators read from target/, relative to ROOT."""
    found = []
    for pattern in ("*/target", "*/*/target"):
        for target in ROOT.glob(pattern):
            found += [p for p in target.iterdir() if p.suffix == ".jar" or p.name.startswith("dependency-tree")]
    return [p.relative_to(ROOT) for p in found]


class Sandbox:
    """A copy of the working tree plus build outputs, safe to break.

    Build outputs are hardlinked, as the hive bundle alone is ~200 MB, so
    rewrite() always replaces a file rather than writing through the link.
    """

    def __init__(self) -> None:
        self.root = pathlib.Path(tempfile.mkdtemp(prefix="license-checks-"))
        tracked = subprocess.run(["git", "ls-files", "-z"], cwd=ROOT, check=True, capture_output=True).stdout
        for name in filter(None, tracked.decode().split("\0")):
            src = ROOT / name
            if src.is_file():
                self._place(src, self.root / name, link=False)
        for rel in build_outputs():
            self._place(ROOT / rel, self.root / rel, link=True)
        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)
        subprocess.run(["git", "add", "-A"], cwd=self.root, check=True)

    @staticmethod
    def _place(src: pathlib.Path, dest: pathlib.Path, link: bool) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            if link:
                os.link(src, dest)
                return
        except OSError:
            pass
        shutil.copy2(src, dest)

    def path(self, rel: str) -> pathlib.Path:
        return self.root / rel

    def one(self, pattern: str) -> pathlib.Path:
        matches = [p for p in self.root.glob(pattern) if not p.name.endswith(("-tests.jar", "-sources.jar"))]
        if len(matches) != 1:
            raise AssertionError(f"expected one match for {pattern}, found {matches}")
        return matches[0]

    def rewrite(self, path: pathlib.Path, text: str) -> None:
        path.unlink()
        path.write_text(text)

    def rewrite_jar(self, path: pathlib.Path, drop: str = "", add: dict[str, str] | None = None) -> None:
        """Copy a jar without the entry named drop, plus the entries in add."""
        tmp = path.with_suffix(".tmp")
        with zipfile.ZipFile(path) as src, zipfile.ZipFile(tmp, "w") as dest:
            for item in src.infolist():
                if item.filename != drop:
                    dest.writestr(item, src.read(item))
            for name, text in (add or {}).items():
                dest.writestr(name, text)
        os.replace(tmp, path)

    def run(self, script: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
        cmd = ["bash" if script.endswith(".sh") else sys.executable, f"{SCRIPTS}/{script}"]
        if env and "PATH" in env:
            cmd[0] = os.path.join(env["PATH"].split(os.pathsep)[0], cmd[0])
        return subprocess.run(cmd, cwd=self.root, capture_output=True, text=True,
                              env={**os.environ, **(env or {})})

    def close(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)


class ValidatorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.box = Sandbox()
        self.addCleanup(self.box.close)

    def assertPasses(self, script: str) -> None:
        result = self.box.run(script)
        self.assertEqual(result.returncode, 0, f"{script} failed on an unmodified tree:\n{result.stdout}{result.stderr}")

    def assertFails(self, script: str, expect: str, env: dict[str, str] | None = None) -> None:
        result = self.box.run(script, env)
        output = result.stdout + result.stderr
        self.assertNotEqual(result.returncode, 0, f"{script} passed despite the planted fault:\n{output}")
        self.assertIn(expect, output)


class CoverageTest(ValidatorTest):
    SCRIPT = "validate_shaded_license_coverage.sh"

    def bare_path(self, broken: str, missing: bool) -> dict[str, str]:
        """A PATH holding only SHELL_TOOLS, with one of them absent or failing."""
        bin_dir = self.box.root / "bare-bin"
        bin_dir.mkdir()
        for tool in SHELL_TOOLS:
            if tool == broken:
                if not missing:
                    (bin_dir / tool).write_text("#!/bin/sh\nexit 2\n")
                    (bin_dir / tool).chmod(0o755)
                continue
            found = shutil.which(tool)
            if found:
                (bin_dir / tool).symlink_to(found)
        return {"PATH": str(bin_dir)}

    def test_unmodified_tree_passes(self) -> None:
        self.assertPasses(self.SCRIPT)

    def test_missing_tool(self) -> None:
        self.assertFails(self.SCRIPT, "required command not found: comm", self.bare_path("comm", missing=True))

    def test_failing_tool(self) -> None:
        # The rg failure mode: a broken tool whose output is consumed indirectly.
        result = self.box.run(self.SCRIPT, self.bare_path("comm", missing=False))
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn("OK   xtable-hive-metastore: license texts match", result.stdout)

    def test_missing_license_text(self) -> None:
        self.box.path("xtable-hive-metastore/src/main/resources/META-INF/licenses/LICENSE-jta").unlink()
        self.assertFails(self.SCRIPT, "LICENSE-jta")

    def test_orphaned_license_text(self) -> None:
        self.box.path("xtable-aws/src/main/resources/META-INF/licenses/LICENSE-not-bundled").write_text("text\n")
        self.assertFails(self.SCRIPT, "LICENSE-not-bundled")

    def test_empty_dependency_tree(self) -> None:
        self.box.rewrite(self.box.path("xtable-aws/target/dependency-tree-runtime.txt"), "")
        self.assertFails(self.SCRIPT, "did not contain any runtime dependencies")


class BundledTextsTest(ValidatorTest):
    SCRIPT = "validate_bundled_license_texts.py"

    def test_unmodified_tree_passes(self) -> None:
        self.assertPasses(self.SCRIPT)

    def test_empty_dependency_tree(self) -> None:
        self.box.rewrite(self.box.path(f"{EXTENSIONS}/target/dependency-tree.txt"), "")
        self.assertFails(self.SCRIPT, "lists no dependencies")

    def test_module_not_built(self) -> None:
        self.box.one(f"{EXTENSIONS}/target/*-bundled.jar").unlink()
        self.assertFails(self.SCRIPT, EXTENSIONS)

    def test_include_missing_from_tree(self) -> None:
        pom = self.box.path(f"{EXTENSIONS}/pom.xml")
        text = pom.read_text().replace("<includes>", "<includes><include>org.example:not-a-dependency</include>", 1)
        self.box.rewrite(pom, text)
        self.assertFails(self.SCRIPT, "org.example:not-a-dependency")

    def test_missing_license_text(self) -> None:
        self.box.rewrite_jar(self.box.one(f"{EXTENSIONS}/target/*-bundled.jar"), drop="META-INF/licenses/LICENSE-paranamer")
        self.assertFails(self.SCRIPT, "paranamer")


class JarLayoutTest(ValidatorTest):
    SCRIPT = "validate_bundled_jar_license_layout.py"

    def test_unmodified_tree_passes(self) -> None:
        self.assertPasses(self.SCRIPT)

    def test_duplicate_bundled_license(self) -> None:
        self.box.rewrite_jar(self.box.one(UTILS_JAR_GLOB), add={"META-INF/LICENSE-bundled": "text\n"})
        self.assertFails(self.SCRIPT, "META-INF/LICENSE-bundled should not be packaged")

    def test_second_license_directory(self) -> None:
        self.box.rewrite_jar(self.box.one(UTILS_JAR_GLOB), add={"license/LICENSE.other.txt": "text\n"})
        self.assertFails(self.SCRIPT, "license texts outside")


if __name__ == "__main__":
    unittest.main(verbosity=2)
