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
## Variables with defaults (if not overwritten by environment)
##
MVN=${MVN:-mvn}

# fail immediately
set -o errexit
set -o nounset
# print command before executing
set -o xtrace

CURR_DIR=`pwd`
if [[ `basename $CURR_DIR` != "release" ]] ; then
  echo "You have to call the script from the release/ dir"
  exit 1
fi

RELEASE_VERSION=`grep -A 5 "<artifactId>xtable</artifactId>" ../pom.xml  | grep '<version>' | sed -e 's/<version>//' -e 's/<\/version>//' -e 's/ //g'`

if [ -z "${RELEASE_VERSION}" ]; then
    echo "RELEASE_VERSION was not set."
    exit 1
fi

echo "RELEASE_VERSION=${RELEASE_VERSION}"

###########################

cd ..

XTABLE_DIR=`pwd`
RELEASE_DIR=${XTABLE_DIR}/src_release
GITHUB_REPO_URL=https://github.com/apache/incubator-xtable.git
RELEASE_TAG=release-${RELEASE_VERSION}-rc${RC_NUM}

echo "Creating source package"

rm -rf ${RELEASE_DIR}
mkdir -p ${RELEASE_DIR}

# Fails if the local tag differs from the pushed one, which voters check against
git fetch ${GITHUB_REPO_URL} tag ${RELEASE_TAG}

# Only committed files minus export-ignore paths in .gitattributes, owned by 0/0 with
# the commit's mtime, so the archive does not depend on the machine that builds it
git -c tar.umask=0022 archive --format=tar --prefix=apache-xtable-${RELEASE_VERSION}/ ${RELEASE_TAG} \
  | gzip -9 --no-name > ${RELEASE_DIR}/apache-xtable-${RELEASE_VERSION}-src.tar.gz
gpg --armor --detach-sig ${RELEASE_DIR}/apache-xtable-${RELEASE_VERSION}-src.tar.gz
cd ${RELEASE_DIR}
shasum -a 512 apache-xtable-${RELEASE_VERSION}-src.tar.gz > apache-xtable-${RELEASE_VERSION}-src.tar.gz.sha512

cd ${CURR_DIR}
