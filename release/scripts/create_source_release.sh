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
## The source release is now a Maven build output: the apache-release profile builds
# xtable-<version>-source-release.tar.gz and .zip with their .asc and .sha512, and
# deploys them to the Nexus staging repository with the jars. See "Build a release
# candidate" in release/release_guide.md.
##
echo "create_source_release.sh is retired: run release/scripts/deploy_staging_jars.sh from a fresh" >&2
echo "clone of the release tag, as described in release/release_guide.md." >&2
exit 1
