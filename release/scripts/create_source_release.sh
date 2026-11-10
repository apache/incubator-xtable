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
# Builds the source release from the pushed RC tag with git archive, then signs and
# checksums it into src_release/.
# Usage: RELEASE_VERSION=<v> RC_NUM=<n> [KEY_ID=<fingerprint>] create_source_release.sh

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

GITHUB_REPO_URL=${GITHUB_REPO_URL:-https://github.com/apache/incubator-xtable.git}
XTABLE_DIR=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
cd "${XTABLE_DIR}"

RELEASE_TAG=release-${RELEASE_VERSION}-rc${RC_NUM}
TAG_COMMIT=$(git rev-parse --verify "refs/tags/${RELEASE_TAG}^{commit}")

# Voters check the archive against the tag on GitHub, so it has to be the same commit
REMOTE_COMMIT=$(git ls-remote "${GITHUB_REPO_URL}" "refs/tags/${RELEASE_TAG}" "refs/tags/${RELEASE_TAG}^{}" | tail -n 1 | cut -f1)
if [ "${REMOTE_COMMIT}" != "${TAG_COMMIT}" ]; then
  echo "${RELEASE_TAG} is ${TAG_COMMIT} locally but '${REMOTE_COMMIT}' on ${GITHUB_REPO_URL}; push the tag first"
  exit 1
fi
if [ "$(git show "${TAG_COMMIT}:pom.xml" | sed -n 's:^    <version>\(.*\)</version>$:\1:p' | head -n 1)" != "${RELEASE_VERSION}" ]; then
  echo "pom.xml version at ${RELEASE_TAG} is not ${RELEASE_VERSION}"
  exit 1
fi

RELEASE_DIR=${XTABLE_DIR}/src_release
ARCHIVE=apache-xtable-${RELEASE_VERSION}-src.tar.gz
rm -rf "${RELEASE_DIR}"
mkdir -p "${RELEASE_DIR}"

# Only committed files, minus export-ignore paths in .gitattributes; owners 0/0 and
# mtimes from the commit, so the tar stream is the same on any machine.
git -c tar.umask=0022 archive --format=tar --prefix="apache-xtable-${RELEASE_VERSION}/" "${TAG_COMMIT}" \
  | gzip -9 --no-name > "${RELEASE_DIR}/${ARCHIVE}"

cd "${RELEASE_DIR}"
gpg ${KEY_ID:+--local-user "${KEY_ID}"} --armor --detach-sig "${ARCHIVE}"
shasum -a 512 "${ARCHIVE}" > "${ARCHIVE}.sha512"
