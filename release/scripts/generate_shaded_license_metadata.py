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
from __future__ import annotations

import pathlib
import re
import xml.etree.ElementTree as ET
import zipfile
from collections import defaultdict
from functools import lru_cache


ROOT = pathlib.Path(__file__).resolve().parents[2]
REPO = pathlib.Path.home() / ".m2" / "repository"
NS = {"m": "http://maven.apache.org/POM/4.0.0"}

# Curated license texts for bundled dependencies whose own jar does not carry a
# license file (asm, protobuf, jamon, ...). One file per artifactId; the text is
# copied verbatim into every shaded module that bundles that dependency.
OVERRIDES_DIR = pathlib.Path(__file__).resolve().parent / "license_overrides"

# The Apache License 2.0 family is already covered in full by the Apache banner
# at the top of LICENSE-bundled, so those dependencies do not need a per-artifact
# license text file.
APACHE_FAMILY = "Apache License 2.0"

# Locations, in priority order, where a dependency jar may carry its own license.
JAR_LICENSE_CANDIDATES = (
    "META-INF/LICENSE",
    "META-INF/LICENSE.txt",
    "META-INF/LICENSE.md",
    "META-INF/LICENSE-MIT",
    "LICENSE",
    "LICENSE.txt",
    "LICENSE.md",
    "license/LICENSE",
    "license.txt",
    "COPYING",
)

FAMILY_ORDER = [
    "Apache License 2.0",
    "Apache Software License 1.1",
    "BSD 3-Clause",
    "BSD 2-Clause",
    "MIT License",
    "Eclipse Distribution License - v 1.0",
    "CDDL + GPLv2 with classpath exception",
    "CDDL",
    "EPL 2.0",
    "EPL 1.0",
    "Common Public License Version 1.0",
    "Mozilla Public License 2.0",
    "Public Domain",
]

APACHE_BANNER = """                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION

   1. Definitions.

      "License" shall mean the terms and conditions for use, reproduction,
      and distribution as defined by Sections 1 through 9 of this document.

      "Licensor" shall mean the copyright owner or entity authorized by
      the copyright owner that is granting the License.

      "Legal Entity" shall mean the union of the acting entity and all
      other entities that control, are controlled by, or are under common
      control with that entity. For the purposes of this definition,
      "control" means (i) the power, direct or indirect, to cause the
      direction or management of such entity, whether by contract or
      otherwise, or (ii) ownership of fifty percent (50%) or more of the
      outstanding shares, or (iii) beneficial ownership of such entity.

      "You" (or "Your") shall mean an individual or Legal Entity
      exercising permissions granted by this License.

      "Source" form shall mean the preferred form for making modifications,
      including but not limited to software source code, documentation
      source, and configuration files.

      "Object" form shall mean any form resulting from mechanical
      transformation or translation of a Source form, including but
      not limited to compiled object code, generated documentation,
      and conversions to other media types.

      "Work" shall mean the work of authorship, whether in Source or
      Object form, made available under the License, as indicated by a
      copyright notice that is included in or attached to the work
      (an example is provided in the Appendix below).

      "Derivative Works" shall mean any work, whether in Source or Object
      form, that is based on (or derived from) the Work and for which the
      editorial revisions, annotations, elaborations, or other modifications
      represent, as a whole, an original work of authorship. For the purposes
      of this License, Derivative Works shall not include works that remain
      separable from, or merely link (or bind by name) to the interfaces of,
      the Work and Derivative Works thereof.

      "Contribution" shall mean any work of authorship, including
      the original version of the Work and any modifications or additions
      to that Work or Derivative Works thereof, that is intentionally
      submitted to Licensor for inclusion in the Work by the copyright owner
      or by an individual or Legal Entity authorized to submit on behalf of
      the copyright owner. For the purposes of this definition, "submitted"
      means any form of electronic, verbal, or written communication sent
      to the Licensor or its representatives, including but not limited to
      communication on electronic mailing lists, source code control systems,
      and issue tracking systems that are managed by, or on behalf of, the
      Licensor for the purpose of discussing and improving the Work, but
      excluding communication that is conspicuously marked or otherwise
      designated in writing by the copyright owner as "Not a Contribution."

      "Contributor" shall mean Licensor and any individual or Legal Entity
      on behalf of whom a Contribution has been received by Licensor and
      subsequently incorporated within the Work.

   2. Grant of Copyright License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      copyright license to reproduce, prepare Derivative Works of,
      publicly display, publicly perform, sublicense, and distribute the
      Work and such Derivative Works in Source or Object form.

   3. Grant of Patent License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      (except as stated in this section) patent license to make, have made,
      use, offer to sell, sell, import, and otherwise transfer the Work,
      where such license applies only to those patent claims licensable
      by such Contributor that are necessarily infringed by their
      Contribution(s) alone or by combination of their Contribution(s)
      with the Work to which such Contribution(s) was submitted. If You
      institute patent litigation against any entity (including a
      cross-claim or counterclaim in a lawsuit) alleging that the Work
      or a Contribution incorporated within the Work constitutes direct
      or contributory patent infringement, then any patent licenses
      granted to You under this License for that Work shall terminate
      as of the date such litigation is filed.

   4. Redistribution. You may reproduce and distribute copies of the
      Work or Derivative Works thereof in any medium, with or without
      modifications, and in Source or Object form, provided that You
      meet the following conditions:

      (a) You must give any other recipients of the Work or
          Derivative Works a copy of this License; and

      (b) You must cause any modified files to carry prominent notices
          stating that You changed the files; and

      (c) You must retain, in the Source form of any Derivative Works
          that You distribute, all copyright, patent, trademark, and
          attribution notices from the Source form of the Work,
          excluding those notices that do not pertain to any part of
          the Derivative Works; and

      (d) If the Work includes a "NOTICE" text file as part of its
          distribution, then any Derivative Works that You distribute must
          include a readable copy of the attribution notices contained
          within such NOTICE file, excluding those notices that do not
          pertain to any part of the Derivative Works, in at least one
          of the following places: within a NOTICE text file distributed
          as part of the Derivative Works; within the Source form or
          documentation, if provided along with the Derivative Works; or,
          within a display generated by the Derivative Works, if and
          wherever such third-party notices normally appear. The contents
          of the NOTICE file are for informational purposes only and
          do not modify the License. You may add Your own attribution
          notices within Derivative Works that You distribute, alongside
          or as an addendum to the NOTICE text from the Work, provided
          that such additional attribution notices cannot be construed
          as modifying the License.

      You may add Your own copyright statement to Your modifications and
      may provide additional or different license terms and conditions
      for use, reproduction, or distribution of Your modifications, or
      for any such Derivative Works as a whole, provided Your use,
      reproduction, and distribution of the Work otherwise complies with
      the conditions stated in this License.

   5. Submission of Contributions. Unless You explicitly state otherwise,
      any Contribution intentionally submitted for inclusion in the Work
      by You to the Licensor shall be under the terms and conditions of
      this License, without any additional terms or conditions.
      Notwithstanding the above, nothing herein shall supersede or modify
      the terms of any separate license agreement you may have executed
      with Licensor regarding such Contributions.

   6. Trademarks. This License does not grant permission to use the trade
      names, trademarks, service marks, or product names of the Licensor,
      except as required for reasonable and customary use in describing the
      origin of the Work and reproducing the content of the NOTICE file.

   7. Disclaimer of Warranty. Unless required by applicable law or
      agreed to in writing, Licensor provides the Work (and each
      Contributor provides its Contributions) on an "AS IS" BASIS,
      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
      implied, including, without limitation, any warranties or conditions
      of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A
      PARTICULAR PURPOSE. You are solely responsible for determining the
      appropriateness of using or redistributing the Work and assume any
      risks associated with Your exercise of permissions under this License.

   8. Limitation of Liability. In no event and under no legal theory,
      whether in tort (including negligence), contract, or otherwise,
      unless required by applicable law (such as deliberate and grossly
      negligent acts) or agreed to in writing, shall any Contributor be
      liable to You for damages, including any direct, indirect, special,
      incidental, or consequential damages of any character arising as a
      result of this License or out of the use or inability to use the
      Work (including but not limited to damages for loss of goodwill,
      work stoppage, computer failure or malfunction, or any and all
      other commercial damages or losses), even if such Contributor
      has been advised of the possibility of such damages.

   9. Accepting Warranty or Additional Liability. While redistributing
      the Work or Derivative Works thereof, You may choose to offer,
      and charge a fee for, acceptance of support, warranty, indemnity,
      or other liability obligations and/or rights consistent with this
      License. However, in accepting such obligations, You may act only
      on Your own behalf and on Your sole responsibility, not on behalf
      of any other Contributor, and only if You agree to indemnify,
      defend, and hold each Contributor harmless for any liability
      incurred by, or claims asserted against, such Contributor by reason
      of your accepting any such warranty or additional liability.

   END OF TERMS AND CONDITIONS

   APPENDIX: How to apply the Apache License to your work.

      To apply the Apache License to your work, attach the following
      boilerplate notice, with the fields enclosed by brackets "[]"
      replaced with your own identifying information. (Don't include
      the brackets!)  The text should be enclosed in the appropriate
      comment syntax for the file format. We also recommend that a
      file or class name and description of purpose be included on the
      same "printed page" as the copyright notice for easier
      identification within third-party archives.

   Copyright [yyyy] [name of copyright owner]

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

---------------------------------------------------------

This product bundles various third-party components under other open source licenses.
This section summarizes those components and their licenses. See licenses/ for text of these licenses.
"""

NOTICE_TEMPLATE = """Apache XTable (incubating)
Copyright 2024-2026 The Apache Software Foundation

This product includes software developed at
The Apache Software Foundation (https://www.apache.org/).

--------------------------------------------------------------------------------

This binary artifact bundles the following projects with NOTICE:
"""


GROUP_OVERRIDES = {
    "aopalliance": "Public Domain",
    "asm": "BSD 3-Clause",
    "commons-el": "Apache Software License 1.1",
    "commons-httpclient": "Apache License 2.0",
    "io.netty": "Apache License 2.0",
    "it.unimi.dsi": "Apache License 2.0",
    "javax.activation": "CDDL + GPLv2 with classpath exception",
    "javax.inject": "Apache License 2.0",
    "javax.mail": "CDDL",
    "javax.servlet": "Apache License 2.0",
    "javax.servlet.jsp": "Apache License 2.0",
    "javax.transaction": "Apache License 2.0",
    "org.apache.hadoop": "Apache License 2.0",
    "org.apache.hbase": "Apache License 2.0",
    "org.apache.velocity": "Apache License 2.0",
    "org.apache.zookeeper": "Apache License 2.0",
    "org.codehaus.jackson": "Apache License 2.0",
    "oro": "Apache Software License 1.1",
    "software.amazon.awssdk": "Apache License 2.0",
    "software.amazon.eventstream": "Apache License 2.0",
    "stax": "CDDL + GPLv2 with classpath exception",
    "xml-apis": "Apache Software License 1.1",
    "xmlenc": "BSD 3-Clause",
}

ARTIFACT_OVERRIDES = {
    ("io.netty", "netty-resolver-dns-native-macos"): "Apache License 2.0",
    ("io.netty", "netty-transport-native-epoll"): "Apache License 2.0",
    ("io.netty", "netty-transport-native-kqueue"): "Apache License 2.0",
    ("org.apache.hive", "hive-exec"): "Apache License 2.0",
    ("org.apache.orc", "orc-core"): "Apache License 2.0",
    ("org.apache.orc", "orc-mapreduce"): "Apache License 2.0",
    ("org.bouncycastle", "bcprov-jdk18on"): "MIT License",
}


def tree_coords(module: pathlib.Path) -> list[tuple[str, str, str]]:
    coords = set()
    for line in (module / "target" / "dependency-tree-runtime.txt").read_text().splitlines():
        line = re.sub(r"^[| +\\-]+", "", line)
        parts = line.split(":")
        if len(parts) >= 5 and parts[2] not in ("pom", "test-jar"):
            coords.add((parts[0], parts[1], parts[3]))
    return sorted(coords)


def shade_modules() -> list[pathlib.Path]:
    modules = []
    for pom_path in sorted(ROOT.rglob("pom.xml")):
        if "<artifactId>maven-shade-plugin</artifactId>" not in pom_path.read_text():
            continue
        if pom_path.parent == ROOT:
            continue
        modules.append(pom_path.parent)
    return modules


def pom_path(group: str, artifact: str, version: str) -> pathlib.Path:
    return REPO / pathlib.Path(group.replace(".", "/")) / artifact / version / f"{artifact}-{version}.pom"


def jar_path(group: str, artifact: str, version: str) -> pathlib.Path:
    return REPO / pathlib.Path(group.replace(".", "/")) / artifact / version / f"{artifact}-{version}.jar"


@lru_cache(maxsize=None)
def licenses_for(group: str, artifact: str, version: str) -> tuple[str, ...]:
    path = pom_path(group, artifact, version)
    if not path.exists():
        return ("__MISSING__",)

    try:
        root = ET.parse(path).getroot()
    except ET.ParseError:
        return ("__PARSE_ERROR__",)

    values = [
        item.text.strip()
        for item in root.findall("./m:licenses/m:license/m:name", NS)
        if item.text and item.text.strip()
    ]
    if values:
        return tuple(values)

    parent = root.find("./m:parent", NS)
    if parent is not None:
        pg = parent.findtext("m:groupId", default="", namespaces=NS).strip()
        pa = parent.findtext("m:artifactId", default="", namespaces=NS).strip()
        pv = parent.findtext("m:version", default="", namespaces=NS).strip()
        if pg and pa and pv and "${" not in pg + pa + pv:
            return licenses_for(pg, pa, pv)

    return ()


def normalize_family(group: str, artifact: str, version: str) -> str:
    key = (group, artifact)
    if key in ARTIFACT_OVERRIDES:
        return ARTIFACT_OVERRIDES[key]
    if group in GROUP_OVERRIDES:
        return GROUP_OVERRIDES[group]

    licenses = licenses_for(group, artifact, version)
    joined = " | ".join(licenses)

    apache_markers = (
        "Apache License",
        "Apache Software License",
        "Apache-2.0",
        "Apache 2",
        "Apache v2",
    )
    if any(marker in joined for marker in apache_markers):
        return "Apache License 2.0"
    if "Apache Software License, Version 1.1" in joined:
        return "Apache Software License 1.1"
    if "MIT" in joined:
        return "MIT License"
    if "Public Domain" in joined:
        return "Public Domain"
    if "Eclipse Distribution License" in joined or "EDL 1.0" in joined:
        return "Eclipse Distribution License - v 1.0"
    if "Mozilla" in joined or "MPL" in joined:
        return "Mozilla Public License 2.0"
    if "Common Public License" in joined:
        return "Common Public License Version 1.0"
    if "EPL 2.0" in joined or "Eclipse Public License 2.0" in joined:
        return "EPL 2.0"
    if "EPL 1.0" in joined or "Eclipse Public License 1.0" in joined or "Eclipse Public License - v 1.0" in joined:
        return "EPL 1.0"
    if "CDDL + GPLv2 with classpath exception" in joined or "CDDL/GPLv2+CE" in joined:
        return "CDDL + GPLv2 with classpath exception"
    if "CDDL" in joined or "GPL2 w/ CPE" in joined:
        return "CDDL + GPLv2 with classpath exception"
    if "BSD 2-Clause" in joined:
        return "BSD 2-Clause"
    if "BSD" in joined or "Go license" in joined:
        return "BSD 3-Clause"

    raise ValueError(f"Unmapped license for {group}:{artifact}:{version}: {licenses}")


def render_license(groups: dict[str, list[tuple[str, str, str]]]) -> str:
    lines = [APACHE_BANNER, ""]
    for family in FAMILY_ORDER:
        coords = groups.get(family)
        if not coords:
            continue
        lines.append(family)
        lines.append("-" * len(family))
        for group, artifact, version in coords:
            lines.append(f"{group}:{artifact}:{version}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def third_party_coords(module: pathlib.Path) -> list[tuple[str, str, str]]:
    return [coord for coord in tree_coords(module) if coord[0] != "org.apache.xtable"]


@lru_cache(maxsize=None)
def notice_text_for(group: str, artifact: str, version: str) -> str | None:
    path = jar_path(group, artifact, version)
    if not path.exists():
        return None

    candidates = (
        "META-INF/NOTICE",
        "META-INF/NOTICE.txt",
        "NOTICE",
        "NOTICE.txt",
    )
    with zipfile.ZipFile(path) as jar_file:
        names = set(jar_file.namelist())
        for candidate in candidates:
            if candidate not in names:
                continue
            text = jar_file.read(candidate).decode("utf-8", errors="replace")
            lines = [line.rstrip() for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
            normalized = "\n".join(lines).strip()
            if normalized:
                return normalized
    return None


@lru_cache(maxsize=None)
def license_text_from_jar(group: str, artifact: str, version: str) -> str | None:
    path = jar_path(group, artifact, version)
    if not path.exists():
        return None
    with zipfile.ZipFile(path) as jar_file:
        names = set(jar_file.namelist())
        for candidate in JAR_LICENSE_CANDIDATES:
            if candidate not in names:
                continue
            text = jar_file.read(candidate).decode("utf-8", errors="replace")
            lines = [line.rstrip() for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
            normalized = "\n".join(lines).strip()
            if normalized:
                return normalized + "\n"
    return None


def license_text_for(group: str, artifact: str, version: str) -> str | None:
    """Return the license text for a bundled dependency.

    A curated override keyed by artifactId is consulted first: it is human
    verified and is required for dependencies whose jar carries no license, or a
    misleading one (e.g. junit bundles Hamcrest's BSD text rather than its own
    Common Public License). Otherwise the dependency's own jar is authoritative.
    Returns None when neither source has a text, so the caller can fail loudly
    rather than ship an unattributed dependency.
    """
    override = OVERRIDES_DIR / f"LICENSE-{artifact}"
    if override.exists():
        content = override.read_text()
        return content if content.endswith("\n") else content + "\n"

    return license_text_from_jar(group, artifact, version)


def bundled_non_apache_coords(license_path: pathlib.Path) -> list[tuple[str, str, str]]:
    """Parse the non-Apache dependency coordinates out of a LICENSE-bundled file.

    License texts are keyed off the committed LICENSE-bundled (the canonical list
    of what the shaded jar bundles) rather than a freshly resolved dependency
    tree, so the texts stay reproducible regardless of the local ~/.m2 state.
    """
    coords: list[tuple[str, str, str]] = []
    family = None
    lines = license_path.read_text().splitlines()
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            family = None
            continue
        if index + 1 < len(lines) and re.fullmatch(r"-{3,}", lines[index + 1].strip()):
            family = stripped
            continue
        if re.fullmatch(r"-{3,}", stripped):
            continue
        if family and family != APACHE_FAMILY and stripped.count(":") == 2:
            group, artifact, version = stripped.split(":")
            coords.append((group, artifact, version))
    return coords


def sync_license_texts(module: pathlib.Path, coords: list[tuple[str, str, str]]) -> list[tuple[str, str, str]]:
    """Regenerate META-INF/licenses/ for a module; return coords with no text."""
    licenses_dir = module / "src" / "main" / "resources" / "META-INF" / "licenses"
    licenses_dir.mkdir(parents=True, exist_ok=True)

    for stale in licenses_dir.glob("LICENSE-*"):
        stale.unlink()

    missing = []
    for group, artifact, version in coords:
        text = license_text_for(group, artifact, version)
        if text is None:
            missing.append((group, artifact, version))
            continue
        (licenses_dir / f"LICENSE-{artifact}").write_text(text)
    return missing


def render_notice(module: pathlib.Path) -> str:
    grouped_notices: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    for coord in third_party_coords(module):
        notice_text = notice_text_for(*coord)
        if notice_text is None:
            continue
        grouped_notices[notice_text].append(coord)

    lines = [NOTICE_TEMPLATE, "--------------------------------------------------------------------------------", ""]
    for notice_text in sorted(grouped_notices):
        for group, artifact, version in grouped_notices[notice_text]:
            lines.append(f"Group: {group} Name: {artifact} Version: {version}")
        lines.append("")
        lines.append("NOTICE:")
        for line in notice_text.splitlines():
            lines.append(f"| {line}" if line else "|")
        lines.append("")
        lines.append("--------------------------------------------------------------------------------")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    missing_texts: dict[pathlib.Path, list[tuple[str, str, str]]] = {}
    for module in shade_modules():
        if not (module / "target" / "dependency-tree-runtime.txt").exists():
            print(
                f"skipping {module.relative_to(ROOT)}: no target/dependency-tree-runtime.txt "
                "(build the module and run dependency:tree first)"
            )
            continue

        groups: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
        for coord in third_party_coords(module):
            family = normalize_family(*coord)
            groups[family].append(coord)

        license_path = module / "src" / "main" / "resources" / "META-INF" / "LICENSE-bundled"
        notice_path = module / "src" / "main" / "resources" / "META-INF" / "NOTICE-bundled"
        license_path.parent.mkdir(parents=True, exist_ok=True)
        license_path.write_text(render_license(groups))
        notice_path.write_text(render_notice(module))

        missing = sync_license_texts(module, bundled_non_apache_coords(license_path))
        if missing:
            missing_texts[module] = missing

    if missing_texts:
        lines = [
            "No license text found for the following bundled dependencies.",
            f"Add a curated text under {OVERRIDES_DIR.relative_to(ROOT)}/<artifactId>:",
        ]
        for module, coords in missing_texts.items():
            lines.append(f"  {module.relative_to(ROOT)}:")
            for group, artifact, version in coords:
                lines.append(f"    - {group}:{artifact}:{version}")
        raise SystemExit("\n".join(lines))


if __name__ == "__main__":
    main()
