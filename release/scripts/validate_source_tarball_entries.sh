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
set -o errexit
set -o nounset

# Reads the archive directly: macOS tar hides ._* entries when listing or extracting
echo "Checking for macOS metadata in $1"
python3 -I - "$1" <<'PY'
import sys, tarfile
bad = [m.name for m in tarfile.open(sys.argv[1]).getmembers()
       if m.name.rsplit("/", 1)[-1].startswith("._") or m.name.endswith(".DS_Store")
       or any(k.startswith(("SCHILY.xattr.", "LIBARCHIVE.xattr.")) for k in m.pax_headers)]
if bad:
    sys.exit(f"\t\t{len(bad)} entries carry macOS metadata, e.g. {bad[:3]} [ERROR]")
PY
echo -e "\t\tNo macOS metadata in the source tarball? - [OK]\n"
