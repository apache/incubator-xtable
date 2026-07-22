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
import sys
import xml.etree.ElementTree as ET
import zipfile
from collections import defaultdict
from functools import lru_cache


ROOT = pathlib.Path(__file__).resolve().parents[2]
REPO = pathlib.Path.home() / ".m2" / "repository"

# Dependencies whose own jar carries no usable license text, so the text has to
# be curated by hand. The text itself lives in the shaded modules' committed
# META-INF/licenses/LICENSE-<artifactId>, which is the copy that ships; keeping a
# second copy in this directory only to feed the generator meant every curated
# text was committed twice.
#
# An artifactId listed here is read from the committed metadata rather than from
# the dependency jar. Anything not listed is taken from its jar, so a dependency
# upgrade still picks up an updated text. Removing a name here without removing
# the file makes the run fail loudly rather than silently ship a stale text.
CURATED_TEXTS = frozenset(
    {
        "ST4",
        "antlr-runtime",
        "aopalliance",
        "jamon-runtime",
        "javolution",
        "jcodings",
        "jersey-client",
        "jersey-guice",
        "jersey-json",
        "jline",
        "joni",
        "leveldbjni-all",
        "paranamer",
        "reactive-streams",
        "stax-api",
        "xmlenc",
        "zstd-jni",
    }
)

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
    "GPL-2.0 with GNU ClasspathException",
    "Public Domain",
]

# A marker that must appear in a dependency's license text for it to be credible
# as that family. Nothing otherwise ties normalize_family() (which reads the POM)
# to the text extracted from the jar, so a jar that ships the wrong license file
# is attributed wrongly and silently -- junit is the known example, and it was
# only caught because someone read it. Any family listed here is checked; a
# family absent from this table is not, so entries can be added incrementally.
#
# Matching is case-insensitive and any one marker is enough.
FAMILY_TEXT_MARKERS = {
    "Apache Software License 1.1": ("Apache Software License",),
    "BSD 3-Clause": ("Redistribution and use in source and binary forms", "BSD"),
    "BSD 2-Clause": ("Redistribution and use in source and binary forms", "BSD"),
    "MIT License": (
        "Permission is hereby granted, free of charge",
        "MIT No Attribution",
        "MIT License",
    ),
    "CDDL": ("COMMON DEVELOPMENT AND DISTRIBUTION LICENSE", "CDDL"),
    "CDDL + GPLv2 with classpath exception": (
        "COMMON DEVELOPMENT AND DISTRIBUTION LICENSE",
        "CDDL",
    ),
    "EPL 1.0": ("Eclipse Public License",),
    "EPL 2.0": ("Eclipse Public License",),
    "Common Public License Version 1.0": ("Common Public License",),
    "Mozilla Public License 2.0": ("Mozilla Public License",),
    "Eclipse Distribution License - v 1.0": (
        "Eclipse Distribution License",
        "Redistribution and use in source and binary forms",
    ),
    "GPL-2.0 with GNU ClasspathException": ("Classpath exception",),
}

# Shade modules the generator does not regenerate metadata for. Kept in step with
# SKIPPED_SHADE_MODULES in validate_shaded_license_coverage.sh: if the validator
# does not gate a module, generating its metadata only produces churn and, for
# xtable-utilities, would require curating license texts for ~30 dependencies of
# an artifact that is not published. See AGENTS.md.
SKIPPED_SHADE_MODULES = ("xtable-utilities",)

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


# Families for dependencies whose POM does not let normalize_family() work them
# out: no <licenses> anywhere in the parent chain, or a name that disagrees with
# the license the ASF has assessed the artifact under.
#
# Keep this table as small as possible. An override that merely repeats what the
# POM already says hides resolution failures instead of surfacing them, so
# anything redundant belongs in the POM lookup, not here.
GROUP_OVERRIDES = {
    "asm": "BSD 3-Clause",
    "commons-el": "Apache Software License 1.1",
    "commons-httpclient": "Apache License 2.0",
    "javax.mail": "CDDL",
    "javax.servlet": "Apache License 2.0",
    "javax.servlet.jsp": "Apache License 2.0",
    "javax.transaction": "Apache License 2.0",
    "org.apache.velocity": "Apache License 2.0",
    "org.apache.zookeeper": "Apache License 2.0",
    "oro": "Apache Software License 1.1",
    "stax": "CDDL + GPLv2 with classpath exception",
}

ARTIFACT_OVERRIDES = {
    # POM declares "Bouncy Castle Licence", which is the MIT text verbatim.
    ("org.bouncycastle", "bcprov-jdk18on"): "MIT License",
    # javolution relicensed from BSD to MIT. The 5.5.1 POM (published 2010)
    # declares "BSD License" pointing at http://javolution.org/LICENSE.txt, which
    # is long dead, and the project has no 5.5.1 tag from which the 2010 text
    # could be recovered. The LICENSE file the project publishes today is MIT,
    # and license_overrides/LICENSE-javolution is that text verbatim, so the
    # family is recorded as MIT to match the text actually shipped. Both are ASF
    # Category A, so this does not change the artifact's licensing category.
    ("javolution", "javolution"): "MIT License",
}


def tree_coords(module: pathlib.Path) -> list[tuple[str, str, str]]:
    """Parse the dependency coordinates out of a text dependency tree.

    Maven prints ``groupId:artifactId:type:version:scope`` and, for an artifact
    with a classifier, ``groupId:artifactId:type:classifier:version:scope``. The
    version is therefore the second-to-last field, never a fixed index: reading
    ``parts[3]`` yields the classifier on the six-field form, which both misses
    the POM lookup and records a classifier where LICENSE-bundled wants a
    version (e.g. ``io.netty:netty-transport-native-epoll:linux-aarch_64``).

    A ``tests`` classifier is excluded for the same reason ``test-jar`` is: its
    type is a plain ``jar``, so the type check alone lets it through.
    """
    coords = set()
    for line in (module / "target" / "dependency-tree-runtime.txt").read_text().splitlines():
        line = re.sub(r"^[| +\\-]+", "", line)
        parts = line.split(":")
        if len(parts) < 5:
            continue
        group, artifact, packaging = parts[0], parts[1], parts[2]
        classifier = parts[3] if len(parts) >= 6 else ""
        version = parts[-2]
        if packaging in ("pom", "test-jar") or classifier == "tests":
            continue
        coords.add((group, artifact, version))
    return sorted(coords)


def shade_modules() -> list[pathlib.Path]:
    modules = []
    for pom_path in sorted(ROOT.rglob("pom.xml")):
        if "<artifactId>maven-shade-plugin</artifactId>" not in pom_path.read_text():
            continue
        if pom_path.parent == ROOT:
            continue
        if pom_path.parent.name in SKIPPED_SHADE_MODULES:
            continue
        modules.append(pom_path.parent)
    return modules


def pom_path(group: str, artifact: str, version: str) -> pathlib.Path:
    return REPO / pathlib.Path(group.replace(".", "/")) / artifact / version / f"{artifact}-{version}.pom"


def jar_path(group: str, artifact: str, version: str) -> pathlib.Path:
    return REPO / pathlib.Path(group.replace(".", "/")) / artifact / version / f"{artifact}-{version}.jar"


def _lenient_pom_scan(text: str) -> tuple[tuple[str, ...], tuple[str, str, str] | None]:
    """Recover license names and the parent coordinate from a non-well-formed POM.

    Maven's own parser is more permissive than expat, so a POM that Maven builds
    against happily can still be rejected as malformed XML. The real case is
    hadoop-project-3.1.0.pom, which contains

        <Xlint:-unchecked/>

    inside a compiler configuration. ``Xlint`` is an undeclared namespace prefix
    and the local name starts with a hyphen, so expat rejects the whole file;
    Maven accepts it. That POM is authentic and not a corrupt download (its
    SHA-1 matches the .sha1 sidecar from Maven Central), and it is the parent of
    the hadoop 3.1.0 yarn artifacts, so without this fallback none of them can
    resolve a license family.

    Only the two facts this script needs are recovered, by text scan: the
    declared license names, and the parent coordinate to continue the walk.
    """
    licenses_block = re.search(r"<licenses>(.*?)</licenses>", text, re.DOTALL)
    names: tuple[str, ...] = ()
    if licenses_block:
        names = tuple(
            name.strip()
            for name in re.findall(r"<name>(.*?)</name>", licenses_block.group(1), re.DOTALL)
            if name.strip()
        )

    parent_block = re.search(r"<parent>(.*?)</parent>", text, re.DOTALL)
    parent: tuple[str, str, str] | None = None
    if parent_block:
        fields = []
        for tag in ("groupId", "artifactId", "version"):
            match = re.search(rf"<{tag}>(.*?)</{tag}>", parent_block.group(1), re.DOTALL)
            fields.append(match.group(1).strip() if match else "")
        if all(fields) and "${" not in "".join(fields):
            parent = (fields[0], fields[1], fields[2])

    return names, parent


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _child_elements(element: ET.Element, name: str) -> list[ET.Element]:
    """Direct children matching ``name``, ignoring XML namespaces.

    POMs in the wild are inconsistent about the Maven namespace. Most declare it
    as the default namespace, but some bind it to a prefix instead, e.g.
    hadoop-yarn-client-2.7.1.pom opens with

        <project xmlns:pom="http://maven.apache.org/POM/4.0.0">

    which leaves every element unqualified. A namespaced XPath silently matches
    nothing on those files, so the whole parent chain looks empty and the
    dependency ends up with no resolvable license family. Matching on local name
    handles both shapes.
    """
    return [child for child in element if _local_name(child.tag) == name]


def _child_text(element: ET.Element, name: str) -> str:
    for child in _child_elements(element, name):
        return (child.text or "").strip()
    return ""


def _declared_licenses(root: ET.Element) -> tuple[str, ...]:
    names = []
    for licenses_element in _child_elements(root, "licenses"):
        for license_element in _child_elements(licenses_element, "license"):
            name = _child_text(license_element, "name")
            if name:
                names.append(name)
    return tuple(names)


def _declared_parent(root: ET.Element) -> tuple[str, str, str] | None:
    for parent in _child_elements(root, "parent"):
        fields = (
            _child_text(parent, "groupId"),
            _child_text(parent, "artifactId"),
            _child_text(parent, "version"),
        )
        if all(fields) and "${" not in "".join(fields):
            return fields
    return None


@lru_cache(maxsize=None)
def licenses_for(group: str, artifact: str, version: str) -> tuple[str, ...]:
    path = pom_path(group, artifact, version)
    if not path.exists():
        return ("__MISSING__",)

    try:
        root = ET.parse(path).getroot()
    except ET.ParseError:
        # Not well-formed to expat but valid enough for Maven; fall back to a
        # text scan rather than giving up on the whole parent chain.
        names, parent = _lenient_pom_scan(path.read_text(encoding="utf-8", errors="strict"))
        if names:
            return names
        if parent is not None:
            return licenses_for(*parent)
        return ("__PARSE_ERROR__",)

    values = _declared_licenses(root)
    if values:
        return values

    parent = _declared_parent(root)
    if parent is not None:
        return licenses_for(*parent)

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
    # GPLv2 on its own is Category X. Only the Classpath Exception makes it
    # usable, so it has to be present explicitly -- never inferred.
    if "GPL" in joined and ("classpath exception" in joined.lower() or "CPE" in joined):
        return "GPL-2.0 with GNU ClasspathException"
    if "BSD 2-Clause" in joined:
        return "BSD 2-Clause"
    if "BSD" in joined or "Go license" in joined:
        return "BSD 3-Clause"

    raise ValueError(f"Unmapped license for {group}:{artifact}:{version}: {licenses}")


def render_license(groups: dict[str, list[tuple[str, str, str]]]) -> str:
    # A family that normalize_family() can return but FAMILY_ORDER does not list
    # would be dropped from LICENSE-bundled without a word, leaving its
    # dependencies with no attribution at all. Refuse instead.
    unordered = sorted(set(groups) - set(FAMILY_ORDER))
    if unordered:
        raise ValueError(
            "License families missing from FAMILY_ORDER, so they would be omitted "
            f"from LICENSE-bundled: {', '.join(unordered)}"
        )

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


@lru_cache(maxsize=None)
def scala_binary_version() -> str:
    match = re.search(
        r"<scala\.binary\.version>([^<]+)</scala\.binary\.version>", (ROOT / "pom.xml").read_text()
    )
    if match is None:
        raise ValueError("scala.binary.version is not set in the root pom.xml")
    return match.group(1).strip()


@lru_cache(maxsize=None)
def shade_includes(module: pathlib.Path) -> frozenset[str]:
    """The ``groupId:artifactId`` entries maven-shade-plugin is told to bundle."""
    artifact_set = re.search(
        r"<artifactSet>(.*?)</artifactSet>", (module / "pom.xml").read_text(), re.DOTALL
    )
    if artifact_set is None:
        raise ValueError(f"{module.relative_to(ROOT)}/pom.xml has no <artifactSet> to read includes from")
    return frozenset(
        include.strip().replace("${scala.binary.version}", scala_binary_version())
        for include in re.findall(r"<include>([^<]+)</include>", artifact_set.group(1))
    )


def third_party_coords(module: pathlib.Path) -> list[tuple[str, str, str]]:
    """Third-party dependencies that actually end up inside the shaded jar.

    A runtime dependency that maven-shade-plugin is not told to include is not
    in the artifact, so it does not belong in the artifact's LICENSE-bundled and
    must not have a license text generated for it. Intersecting the runtime tree
    with the include list keeps the metadata describing the jar rather than the
    classpath.

    Keeping the include list itself in step with the runtime tree is a separate
    concern, enforced independently by validate_shaded_license_coverage.sh.
    """
    includes = shade_includes(module)
    return [
        coord
        for coord in tree_coords(module)
        if coord[0] != "org.apache.xtable" and f"{coord[0]}:{coord[1]}" in includes
    ]


class UndecodableText(Exception):
    """A bundled LICENSE/NOTICE entry is not valid UTF-8."""

    def __init__(self, coord: tuple[str, str, str], entry: str, reason: str) -> None:
        super().__init__(f"{':'.join(coord)} {entry}: {reason}")
        self.coord = coord
        self.entry = entry
        self.reason = reason


def _decode_entry(raw: bytes, coord: tuple[str, str, str], entry: str) -> str:
    """Decode a jar entry as UTF-8, refusing to guess at anything else.

    ``errors="replace"`` would substitute U+FFFD for every byte that is not
    valid UTF-8, which silently mangles the older latin-1 license texts (the
    copyright sign in javax.activation and javax.mail, for two that are actually
    bundled here) and then ships the mangled result as that dependency's
    license. Raising instead routes the artifact onto the "needs a curated
    override" list, where a human decides what text is correct.
    """
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise UndecodableText(coord, entry, str(error)) from error
    lines = [line.rstrip() for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    return "\n".join(lines).strip()


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
            normalized = _decode_entry(jar_file.read(candidate), (group, artifact, version), candidate)
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
            normalized = _decode_entry(jar_file.read(candidate), (group, artifact, version), candidate)
            if normalized:
                return normalized + "\n"

        # Last resort: a license under a non-standard name at the jar root or
        # directly in META-INF. junit-4.12.jar, for one, ships its Eclipse
        # Public License as LICENSE-junit.txt, which no fixed candidate matches,
        # and the alternative is a curated override that then has to be kept
        # correct by hand across version bumps.
        for candidate in sorted(names):
            stem = candidate.rsplit("/", 1)[-1]
            if candidate.count("/") > (1 if candidate.startswith("META-INF/") else 0):
                continue
            if not re.fullmatch(r"(?i)licen[sc]e([-_.].*)?", stem):
                continue
            normalized = _decode_entry(jar_file.read(candidate), (group, artifact, version), candidate)
            if normalized:
                return normalized + "\n"
    return None


def text_matches_family(family: str, text: str) -> bool:
    """Whether a license text is credible as ``family``.

    normalize_family() reads the POM and the text comes from the jar, with
    nothing tying the two together. A jar that ships someone else's license file
    would otherwise be attributed wrongly and silently: junit 4.11 ships
    Hamcrest's BSD text, and jol-core ships the plain GPLv2 text even though the
    Classpath Exception in its POM is what makes it usable at all. Families with
    no entry in FAMILY_TEXT_MARKERS are not checked.
    """
    markers = FAMILY_TEXT_MARKERS.get(family)
    if not markers:
        return True
    # License texts are hand-wrapped, so a marker phrase can be split across
    # lines or double-spaced (slf4j's MIT text reads "free  of charge"). Compare
    # with runs of whitespace collapsed so that formatting is not mistaken for a
    # different license.
    flattened = re.sub(r"\s+", " ", text).lower()
    return any(re.sub(r"\s+", " ", marker).lower() in flattened for marker in markers)


@lru_cache(maxsize=None)
def curated_text(artifact: str) -> str | None:
    """The committed META-INF/licenses text for a hand-curated dependency.

    Read before anything is written, so it does not matter that
    write_license_texts() later clears the directory it came from. Any shaded
    module may hold the copy: a dependency bundled by two modules is curated
    once and reused, which is why the lookup is not scoped to one module.
    """
    for module in shade_modules():
        path = module / "src" / "main" / "resources" / "META-INF" / "licenses" / f"LICENSE-{artifact}"
        if path.exists():
            content = path.read_text(encoding="utf-8", errors="strict")
            return content if content.endswith("\n") else content + "\n"
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
    if artifact in CURATED_TEXTS:
        return curated_text(artifact)

    try:
        return license_text_from_jar(group, artifact, version)
    except UndecodableText:
        return None


def bundled_non_apache_coords(license_path: pathlib.Path) -> list[tuple[str, str, str]]:
    """Parse the non-Apache dependency coordinates out of a LICENSE-bundled file.

    This is the single implementation of that parse. It is no longer how the
    generator decides which texts to write -- main() uses the family groups it
    already has in memory -- but validate_shaded_license_coverage.sh needs to
    read the committed file, and it calling this (via --non-apache-artifact-ids)
    is what keeps generator and validator from disagreeing about which
    dependencies need a license text.
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


def resolve_license_texts(
    coords: list[tuple[str, str, str]],
    families: dict[tuple[str, str, str], str],
) -> tuple[dict[str, str], list[tuple[str, str, str]], list[tuple[tuple[str, str, str], str]]]:
    """Resolve every license text for a module without touching the tree.

    Returns ``(filename -> text, coords with no text, (coord, family) pairs whose
    text does not look like the family)``. Nothing is written or deleted here, so
    a module whose texts cannot all be resolved leaves the working tree exactly
    as it was.
    """
    resolved: dict[str, str] = {}
    missing: list[tuple[str, str, str]] = []
    mismatched: list[tuple[tuple[str, str, str], str]] = []

    for coord in coords:
        text = license_text_for(*coord)
        if text is None:
            missing.append(coord)
            continue
        family = families[coord]
        if not text_matches_family(family, text):
            mismatched.append((coord, family))
            continue
        resolved[f"LICENSE-{coord[1]}"] = text

    return resolved, missing, mismatched


def write_license_texts(module: pathlib.Path, resolved: dict[str, str]) -> None:
    """Replace META-INF/licenses/ with exactly the already-resolved texts."""
    licenses_dir = module / "src" / "main" / "resources" / "META-INF" / "licenses"
    licenses_dir.mkdir(parents=True, exist_ok=True)

    for stale in licenses_dir.glob("LICENSE-*"):
        stale.unlink()

    for name, text in resolved.items():
        (licenses_dir / name).write_text(text, encoding="utf-8")


def render_notice(module: pathlib.Path) -> tuple[str, list[UndecodableText]]:
    grouped_notices: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    undecodable: list[UndecodableText] = []
    for coord in third_party_coords(module):
        try:
            notice_text = notice_text_for(*coord)
        except UndecodableText as error:
            undecodable.append(error)
            continue
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

    return "\n".join(lines).rstrip() + "\n", undecodable


def plan_module(module: pathlib.Path) -> tuple[dict[pathlib.Path, str], dict[str, str], list, list, list]:
    """Work out everything a module's regeneration would write, writing nothing.

    Returns ``(path -> content, license texts, missing, mismatched, undecodable)``.
    """
    groups: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    families: dict[tuple[str, str, str], str] = {}
    for coord in third_party_coords(module):
        family = normalize_family(*coord)
        groups[family].append(coord)
        families[coord] = family

    notice_text, undecodable = render_notice(module)
    meta_inf = module / "src" / "main" / "resources" / "META-INF"
    contents = {
        meta_inf / "LICENSE-bundled": render_license(groups),
        meta_inf / "NOTICE-bundled": notice_text,
    }

    # The coordinates needing a text come straight from the family groups above.
    # Rendering LICENSE-bundled and parsing it back to recover the same list
    # would be a round-trip through a text format for data already in hand.
    non_apache = [coord for family, coords in groups.items() if family != APACHE_FAMILY for coord in coords]
    resolved, missing, mismatched = resolve_license_texts(non_apache, families)

    return contents, resolved, missing, mismatched, undecodable


def main() -> None:
    if len(sys.argv) > 1:
        if sys.argv[1] == "--non-apache-artifact-ids" and len(sys.argv) == 3:
            coords = bundled_non_apache_coords(pathlib.Path(sys.argv[2]))
            print("\n".join(sorted({artifact for _, artifact, _ in coords})))
            return
        raise SystemExit(
            f"usage: {pathlib.Path(sys.argv[0]).name} [--non-apache-artifact-ids <LICENSE-bundled>]"
        )

    modules = shade_modules()
    missing_trees = [m for m in modules if not (m / "target" / "dependency-tree-runtime.txt").exists()]

    # Regenerating only the modules that happen to have a dependency tree
    # produces a diff that looks like a full regeneration but is not, and this
    # drives release artifacts. Refuse the whole run instead.
    if missing_trees:
        names = ",".join(str(m.relative_to(ROOT)) for m in missing_trees)
        raise SystemExit(
            "No target/dependency-tree-runtime.txt for: "
            + ", ".join(str(m.relative_to(ROOT)) for m in missing_trees)
            + "\nNothing was regenerated. Produce the trees first:\n"
            f"  ./mvnw -pl {names} -am -DskipTests dependency:tree "
            "-Dscope=runtime -DoutputType=text -DoutputFile=target/dependency-tree-runtime.txt"
        )

    # Resolve every module before writing anything. A failure part-way through
    # used to leave texts deleted and LICENSE-bundled/NOTICE-bundled rewritten
    # for the modules already processed, so the only recovery was to throw away
    # the whole run with git checkout.
    planned: dict[pathlib.Path, tuple[dict[pathlib.Path, str], dict[str, str]]] = {}
    missing_texts: dict[pathlib.Path, list[tuple[str, str, str]]] = {}
    mismatched_texts: dict[pathlib.Path, list[tuple[tuple[str, str, str], str]]] = {}
    undecodable_notices: dict[pathlib.Path, list[UndecodableText]] = {}

    for module in modules:
        contents, resolved, missing, mismatched, undecodable = plan_module(module)
        planned[module] = (contents, resolved)
        if missing:
            missing_texts[module] = missing
        if mismatched:
            mismatched_texts[module] = mismatched
        if undecodable:
            undecodable_notices[module] = undecodable

    if missing_texts or mismatched_texts or undecodable_notices:
        lines = ["Nothing was regenerated; the working tree is unchanged."]
        if missing_texts:
            lines.append("")
            lines.append("No license text found for these bundled dependencies.")
            lines.append(
                "Commit the text as "
                "<module>/src/main/resources/META-INF/licenses/LICENSE-<artifactId> "
                "and add the artifactId to CURATED_TEXTS in this script:"
            )
            for module, coords in missing_texts.items():
                lines.append(f"  {module.relative_to(ROOT)}:")
                for coord in coords:
                    lines.append(f"    - {':'.join(coord)}")
        if mismatched_texts:
            lines.append("")
            lines.append("License text does not match the family resolved from the POM.")
            lines.append("Either the jar ships the wrong license file (curate an override) or the")
            lines.append("family is wrong (fix the override table or FAMILY_TEXT_MARKERS):")
            for module, entries in mismatched_texts.items():
                lines.append(f"  {module.relative_to(ROOT)}:")
                for coord, family in entries:
                    lines.append(f"    - {':'.join(coord)} resolved as {family!r}")
        if undecodable_notices:
            lines.append("")
            lines.append("NOTICE entries that are not valid UTF-8:")
            for module, errors in undecodable_notices.items():
                lines.append(f"  {module.relative_to(ROOT)}:")
                for error in errors:
                    lines.append(f"    - {error}")
        raise SystemExit("\n".join(lines))

    for module, (contents, resolved) in planned.items():
        for path, content in contents.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
        write_license_texts(module, resolved)
        print(f"regenerated {module.relative_to(ROOT)}: {len(resolved)} license texts")


if __name__ == "__main__":
    main()
