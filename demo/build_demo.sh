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
## Build the XTable jars required for the demo and generate the dependency
## version properties consumed by the demo notebooks.

set -e

CURRENT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
XTABLE_HOME="$( cd "$(dirname "$CURRENT_DIR")" ; pwd -P )"
cd "$XTABLE_HOME"

## Read a property from the root pom so the demo always matches the versions
## xtable is built against.
get_property() {
  ./mvnw -q help:evaluate -Dexpression="$1" -DforceStdout
}

PROJECT_VERSION=$(get_property project.version)
SCALA_BINARY_VERSION=$(get_property scala.binary.version)

./mvnw install -am -pl xtable-core -DskipTests -T 2

mkdir -p demo/jars
cp "xtable-hudi-support/xtable-hudi-support-utils/target/xtable-hudi-support-utils-${PROJECT_VERSION}.jar" demo/jars
cp "xtable-api/target/xtable-api-${PROJECT_VERSION}.jar" demo/jars
cp "xtable-core/target/xtable-core_${SCALA_BINARY_VERSION}-${PROJECT_VERSION}.jar" demo/jars

## Export the dependency versions used by the notebooks. The notebooks read
## this file from /home/jars/versions.properties instead of hardcoding versions.
{
  cat style/text-license-header
  echo "scala.binary.version=${SCALA_BINARY_VERSION}"
  for prop in spark.version spark.version.prefix hudi.version delta.version delta.kernel.version iceberg.hive.runtime.version; do
    echo "${prop}=$(get_property "${prop}")"
  done
} > demo/jars/versions.properties

echo "XTable demo jars and versions.properties generated under demo/jars:"
ls demo/jars
