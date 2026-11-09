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
# print command before executing
#set -o xtrace

CURR_DIR=`pwd`
if [[ `basename $CURR_DIR` != "scripts" ]] ; then
  echo "You have to call the script from the scripts/ dir"
  exit 1
fi

REDIRECT=' > /dev/null 2>&1'
if [[ $# -lt 1 ]]; then
    echo "This script will validate source release candidate published in dist for apache xtable (incubating)"
    echo "You can set 3 args to this script: release version is mandatory, while rest two are optional. Default release_type is \"dev\". Or you can set to \"release\""
    echo "--release=\${CURRENT_RELEASE_VERSION}"
    echo "--rc_num=\${RC_NUM}"
    echo "--release_type=release"
    echo "To validate files on disk instead of dist.apache.org, for example target/ of a release build:"
    echo "--local_dir=<directory holding xtable-<version>-source-release.*>"
    exit
else
    for param in "$@"
    do
	if [[ $param =~ --release\=([0-9]\.[0-9]*\.[0-9].*) ]]; then
		RELEASE_VERSION=${BASH_REMATCH[1]}
	fi
	if [[ $param =~ --rc_num\=([0-9]*) ]]; then
                RC_NUM=${BASH_REMATCH[1]}
        fi
        if [[ $param =~ --release_type\="release" ]]; then
                RELEASE_TYPE="release"
        fi
        if [[ $param =~ --local_dir\=(.+) ]]; then
                LOCAL_DIR=$(cd "${BASH_REMATCH[1]}" && pwd)
        fi
	if [[ $param =~ --verbose ]]; then
               REDIRECT=""
        fi
    done
fi

if [ -z ${RC_NUM+x} ]; then
   RC_NUM=-1
fi

if [ -z ${RELEASE_TYPE+x} ]; then
   RELEASE_TYPE=dev
fi

WORK_DIR=/tmp/validation_scratch_dir_001
rm -rf $WORK_DIR
mkdir $WORK_DIR
pushd $WORK_DIR

LOCAL_SVN_DIR=local_svn_dir
ROOT_SVN_URL=https://dist.apache.org/repos/dist/
REPO_TYPE=${RELEASE_TYPE}
XTABLE_REPO=incubator/xtable

if [ $RC_NUM == -1 ]; then
    ARTIFACT_SUFFIX=${RELEASE_VERSION}
else
    ARTIFACT_SUFFIX=${RELEASE_VERSION}-rc${RC_NUM}
fi

if [ $RELEASE_TYPE != "release" ] && [ $RELEASE_TYPE != "dev" ]; then
  echo "Unexpected RELEASE_TYPE: $RELEASE_TYPE"
  exit 1;
fi

rm -rf $LOCAL_SVN_DIR
mkdir $LOCAL_SVN_DIR
cd $LOCAL_SVN_DIR

echo "Current directory: `pwd`"

if [ -n "${LOCAL_DIR:-}" ]; then
  echo "Copying from $LOCAL_DIR"
  mkdir ${ARTIFACT_SUFFIX}
  cp "$LOCAL_DIR"/xtable-${RELEASE_VERSION}-source-release.* ${ARTIFACT_SUFFIX}/
else
  FULL_SVN_URL=${ROOT_SVN_URL}/${REPO_TYPE}/${XTABLE_REPO}/${ARTIFACT_SUFFIX}

  echo "Downloading from svn co $FULL_SVN_URL"

  (bash -c "svn co $FULL_SVN_URL $REDIRECT") || (echo -e "\t\t Unable to checkout  $FULL_SVN_URL to $REDIRECT. Please run with --verbose to get details\n" && exit -1)
fi

SOURCE_RELEASE=xtable-${RELEASE_VERSION}-source-release
TOP_DIR=xtable-${RELEASE_VERSION}
echo "Validating ${SOURCE_RELEASE} with release type \"${REPO_TYPE}\""
cd ${ARTIFACT_SUFFIX}

if [ ! -f ${SOURCE_RELEASE}.tar.gz ]; then
  echo -e "\t\t${SOURCE_RELEASE}.tar.gz not found - [ERROR]\n"
  exit 1
fi
ARCHIVES=${SOURCE_RELEASE}.tar.gz
if [ -f ${SOURCE_RELEASE}.zip ]; then
  ARCHIVES="${ARCHIVES} ${SOURCE_RELEASE}.zip"
fi

echo "Checking Checksum of Source Release"
for archive in $ARCHIVES; do
  (bash -c "shasum -a 512 -c ${archive}.sha512 $REDIRECT") || (echo -e "\t\tChecksum Check of ${archive} - [ERROR]\n" && exit 1)
done
echo -e "\t\tChecksum Check of Source Release - [OK]\n"

if [ -n "${LOCAL_DIR:-}" ] && [ ! -f ${SOURCE_RELEASE}.tar.gz.asc ]; then
  echo -e "\t\tNo signature next to the local files, skipping the Signature Check\n"
else
  if [ -z "${LOCAL_DIR:-}" ]; then
    curl https://dist.apache.org/repos/dist/dev/incubator/xtable/KEYS > ../KEYS
    bash -c "gpg --import ../KEYS $REDIRECT"
  fi

  echo "Checking Signature"
  for archive in $ARCHIVES; do
    (bash -c "gpg --verify ${archive}.asc ${archive} $REDIRECT") || (echo -e "\t\tSignature Check of ${archive} - [ERROR]\n\t\t Run with --verbose to get details\n" && exit 1)
  done
  echo -e "\t\tSignature Check - [OK]\n"
fi

# RC1 of 0.5.0 was packed by bsdtar on macOS and carried AppleDouble files and extended
# attributes; check the archive entries rather than what a particular tar prints.
echo "Checking Source Release entries"
(python3 - "$TOP_DIR" $ARCHIVES <<'PYEOF'
import sys, tarfile, zipfile

top, archives = sys.argv[1], sys.argv[2:]
listings = {}
errors = []
for archive in archives:
    if archive.endswith(".zip"):
        with zipfile.ZipFile(archive) as zf:
            names = [i.filename.rstrip("/") for i in zf.infolist()]
    else:
        names = []
        with tarfile.open(archive, "r:gz") as tf:
            for member in tf:
                names.append(member.name.rstrip("/"))
                headers = [k for k in member.pax_headers if k not in ("path", "linkpath", "size", "mtime", "uid", "gid", "uname", "gname")]
                if headers:
                    errors.append(f"{archive}: {member.name}: pax headers {headers}")
                if member.uid != 0 or member.gid != 0:
                    errors.append(f"{archive}: {member.name}: owner {member.uid}/{member.gid}")
    for name in names:
        parts = name.split("/")
        if parts[0] != top:
            errors.append(f"{archive}: {name}: outside {top}/")
        if any(p.startswith("._") or p in ("__MACOSX", ".DS_Store") for p in parts):
            errors.append(f"{archive}: {name}: macOS metadata")
    listings[archive] = set(names)
if len(listings) == 2:
    tgz, zipped = listings.values()
    for name in sorted(tgz ^ zipped):
        errors.append(f"{name}: only in one of {', '.join(archives)}")
for error in errors[:50]:
    print(error)
if errors:
    print(f"{len(errors)} problems")
sys.exit(1 if errors else 0)
PYEOF
) || (echo -e "\t\tSource Release entries Check - [ERROR]\n" && exit 1)
echo -e "\t\tNo macOS metadata, extended attributes or stray paths ? [OK]\n"

(bash -c "tar -zxf ${SOURCE_RELEASE}.tar.gz $REDIRECT") || (echo -e "\t\t Unable to untar ${SOURCE_RELEASE}.tar.gz - [ERROR]\n\t\t Please run with --verbose to get details\n" && exit 1)
cd ${TOP_DIR}

$CURR_DIR/validate_source_binary_files.sh

$CURR_DIR/validate_source_copyright.sh

$CURR_DIR/validate_source_rat.sh

popd
