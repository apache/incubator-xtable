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
MVN=${MVN:-./mvnw}
# fail immediately
set -o errexit
set -o nounset

CURR_DIR=$(pwd)
if [ ! -d "$CURR_DIR/packaging" ] ; then
  echo "You have to call the script from the repository root dir that contains 'packaging/'"
  exit 1
fi

if [ "$#" -gt "1" ]; then
  echo "Only accept 0 or 1 argument. Use -h to see examples."
  exit 1
fi


if [ "${1:-}" == "-h" ]; then
  echo "
Usage: $(basename "$0") [OPTIONS]

Options:
-h, --help
"
  exit 0
fi


# -Dmaven.build.cache.enabled=false: the build cache (.mvn/extensions.xml) restores the
# cached source:jar-no-fork execution and then re-attaches the sources jar, which fails the
# release build with "duplicated artifacts attached". Disable it for release deploys.
COMMON_OPTIONS="-DdeployArtifacts=true -DskipTests -DretryFailedDeploymentCount=10 -Dmaven.build.cache.enabled=false"
echo "Cleaning everything before any deployment"
$MVN clean $COMMON_OPTIONS
echo "Building with options"
$MVN install $COMMON_OPTIONS

echo "Deploying to repository.apache.org with version options"
$MVN deploy $COMMON_OPTIONS
