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
## Pre-fetch the demo notebook dependencies into the coursier cache at image
## build time so the notebooks' first cell resolves instantly. The dependency
## versions are read from the versions.properties file passed as the first
## argument (generated from the root pom by build_demo.sh).
set -e

PROPS_FILE="$1"

prop() {
  grep "^$1=" "${PROPS_FILE}" | cut -d= -f2
}

SCALA_BINARY_VERSION=$(prop scala.binary.version)
SPARK_VERSION=$(prop spark.version)
SPARK_VERSION_PREFIX=$(prop spark.version.prefix)
HUDI_VERSION=$(prop hudi.version)
DELTA_VERSION=$(prop delta.version)
DELTA_KERNEL_VERSION=$(prop delta.kernel.version)
ICEBERG_HIVE_RUNTIME_VERSION=$(prop iceberg.hive.runtime.version)

coursier fetch --quiet \
  "org.apache.logging.log4j:log4j-api:2.17.2" \
  "org.apache.logging.log4j:log4j-core:2.17.2" \
  "org.apache.spark:spark-sql_${SCALA_BINARY_VERSION}:${SPARK_VERSION}" \
  "org.apache.spark:spark-hive_${SCALA_BINARY_VERSION}:${SPARK_VERSION}" \
  "org.apache.hudi:hudi-spark${SPARK_VERSION_PREFIX}-bundle_${SCALA_BINARY_VERSION}:${HUDI_VERSION}" \
  "org.apache.hudi:hudi-java-client:${HUDI_VERSION}" \
  "io.delta:delta-core_${SCALA_BINARY_VERSION}:${DELTA_VERSION}" \
  "io.delta:delta-kernel-api:${DELTA_KERNEL_VERSION}" \
  "io.delta:delta-kernel-defaults:${DELTA_KERNEL_VERSION}" \
  "org.apache.iceberg:iceberg-hive-runtime:${ICEBERG_HIVE_RUNTIME_VERSION}" \
  "io.trino:trino-jdbc:431" \
  "com.facebook.presto:presto-jdbc:0.283"

echo "Notebook dependencies pre-fetched into the coursier cache."
