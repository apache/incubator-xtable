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
CLONE_DIR=${RELEASE_DIR}/incubator-xtable-tmp-clone
GITHUB_REPO_URL=git@github.com:apache/incubator-xtable.git

echo "Creating source package"

rm -rf ${RELEASE_DIR}
mkdir -p ${RELEASE_DIR}

# create a temporary git clone to ensure that we have a pristine source release
git clone ${GITHUB_REPO_URL} ${CLONE_DIR}
cd ${CLONE_DIR}
git checkout release-${RELEASE_VERSION}-rc${RC_NUM}

rsync -a \
  --exclude ".git" --exclude ".gitignore" --exclude ".gitattributes" --exclude ".travis.yml" \
  --exclude ".github" --exclude "target" --exclude ".idea" --exclude "*.iml" --exclude ".DS_Store" \
  --exclude "build-target" --exclude ".rubydeps" --exclude "rfc" --exclude "docker/images" \
  --exclude "assets" --exclude "demo" --exclude "website" --exclude "style/IDE.png" . apache-xtable-$RELEASE_VERSION

tar czf ${RELEASE_DIR}/apache-xtable-${RELEASE_VERSION}.src.tgz apache-xtable-$RELEASE_VERSION
gpg --armor --local-user E391B3E8179C4FD9BB8BF72002AB8E945EFD1E91 --detach-sig ${RELEASE_DIR}/apache-xtable-${RELEASE_VERSION}.src.tgz
cd ${RELEASE_DIR}
shasum -a 512 apache-xtable-${RELEASE_VERSION}.src.tgz > apache-xtable-${RELEASE_VERSION}.src.tgz.sha512

cd ${CURR_DIR}
rm -rf ${CLONE_DIR}
