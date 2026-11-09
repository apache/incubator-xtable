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
## Builds and deploys a release candidate to the Apache Nexus staging repository: the jars,
# sources and javadoc, plus the source release (tar.gz and zip), all signed, with the SHA-512
# of the source release. Run it from the root of a fresh clone of the release tag; extra
# arguments are passed to Maven, for example -Dgpg.keyname=<FINGERPRINT>.
##
MVN=${MVN:-./mvnw}

set -o errexit
set -o nounset

if [ "${1:-}" == "-h" ] || [ "${1:-}" == "--help" ]; then
  echo "Usage: $(basename "$0") [MAVEN_OPTIONS]"
  echo "Run from the root of a fresh clone of the release tag, for example"
  echo "  git clone --branch release-\${RELEASE_VERSION}-rc\${RC_NUM} https://github.com/apache/incubator-xtable.git"
  exit 0
fi

if [ ! -f pom.xml ] || [ ! -x mvnw ] || [ ! -d release/assembly ]; then
  echo "You have to call the script from the root of the incubator-xtable checkout" >&2
  exit 1
fi

# The source release packs the working directory, so it must hold exactly the tag.
if ! TAG=$(git describe --exact-match --tags HEAD 2>/dev/null); then
  echo "HEAD is not at a tag; check out release-<version>-rc<N> first" >&2
  exit 1
fi
if [ -n "$(git status --porcelain --ignored --untracked-files=all)" ]; then
  echo "The checkout has modified, untracked or ignored files; use a fresh clone of ${TAG}:" >&2
  git status --short --ignored --untracked-files=all | head -20 >&2
  exit 1
fi

echo "Deploying ${TAG} to repository.apache.org"
$MVN clean deploy -Papache-release -DskipTests -DretryFailedDeploymentCount=10 -Dmaven.build.cache.enabled=false "$@"
