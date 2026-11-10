#!/bin/bash
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
# Usage: validate_source_tarball_entries.sh <archive> <top-level directory>
set -o errexit
set -o nounset

# Reads the archive directly: macOS tar hides ._* entries when listing or extracting
echo "Checking the entries of $1"
python3 -I - "$1" "$2" <<'PY'
import sys, tarfile
archive, top = sys.argv[1], sys.argv[2]
# git archive writes the commit id as a global "comment" header; tars that copy
# extended attributes add SCHILY.xattr.* or LIBARCHIVE.xattr.* keys
allowed_pax = {"comment", "path", "linkpath", "size", "mtime", "uid", "gid", "uname", "gname"}
problems = []
for m in tarfile.open(archive).getmembers():
    base = m.name.rstrip("/").rsplit("/", 1)[-1]
    if base.startswith("._") or base in (".DS_Store", "__MACOSX"):
        problems.append(f"{m.name}: macOS metadata")
    if m.name != top and not m.name.startswith(top + "/") or ".." in m.name.split("/"):
        problems.append(f"{m.name}: outside {top}/")
    if (m.uid, m.gid) != (0, 0):
        problems.append(f"{m.name}: owner {m.uid}/{m.gid}")
    if set(m.pax_headers) - allowed_pax:
        problems.append(f"{m.name}: pax headers {sorted(set(m.pax_headers) - allowed_pax)}")
if problems:
    sys.exit(f"\t\t{len(problems)} bad entries, e.g. {problems[:3]} [ERROR]")
PY
echo -e "\t\tArchive entries clean? - [OK]\n"
