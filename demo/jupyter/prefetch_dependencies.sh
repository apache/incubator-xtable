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

## Fail on a missing or renamed key rather than substituting an empty string:
## otherwise a typo, or build_demo.sh dropping a property, produces a coordinate
## like "org.apache.spark:spark-sql_:" and a coursier resolution error that says
## nothing about the real cause. Mirrors what predef.sc does.
prop() {
  local value
  value=$(grep "^$1=" "${PROPS_FILE}" | cut -d= -f2-)
  if [ -z "${value}" ]; then
    echo "ERROR: $1 missing from ${PROPS_FILE}; re-run build_demo.sh" >&2
    exit 1
  fi
  echo "${value}"
}

SCALA_BINARY_VERSION=$(prop scala.binary.version)
SPARK_VERSION=$(prop spark.version)
SPARK_VERSION_PREFIX=$(prop spark.version.prefix)
HUDI_VERSION=$(prop hudi.version)
DELTA_VERSION=$(prop delta.version)
DELTA_KERNEL_VERSION=$(prop delta.kernel.version)
ICEBERG_HIVE_RUNTIME_VERSION=$(prop iceberg.hive.runtime.version)
LOG4J_VERSION=$(prop log4j.version)
TRINO_JDBC_VERSION=$(prop trino.jdbc.version)
PRESTO_JDBC_VERSION=$(prop presto.jdbc.version)

## Every coordinate's version comes from versions.properties. This list and the
## one in predef.sc must resolve to the same artifacts: this one warms the
## coursier cache at image build time, that one resolves at kernel startup, so a
## divergence of one version sends every kernel start back to the network -
## exactly the slow path this image exists to avoid.
coursier fetch --quiet \
  "org.apache.logging.log4j:log4j-api:${LOG4J_VERSION}" \
  "org.apache.logging.log4j:log4j-core:${LOG4J_VERSION}" \
  "org.apache.spark:spark-sql_${SCALA_BINARY_VERSION}:${SPARK_VERSION}" \
  "org.apache.spark:spark-hive_${SCALA_BINARY_VERSION}:${SPARK_VERSION}" \
  "org.apache.hudi:hudi-spark${SPARK_VERSION_PREFIX}-bundle_${SCALA_BINARY_VERSION}:${HUDI_VERSION}" \
  "org.apache.hudi:hudi-java-client:${HUDI_VERSION}" \
  "io.delta:delta-spark_${SCALA_BINARY_VERSION}:${DELTA_VERSION}" \
  "io.delta:delta-kernel-api:${DELTA_KERNEL_VERSION}" \
  "io.delta:delta-kernel-defaults:${DELTA_KERNEL_VERSION}" \
  "org.apache.iceberg:iceberg-hive-runtime:${ICEBERG_HIVE_RUNTIME_VERSION}" \
  "io.trino:trino-jdbc:${TRINO_JDBC_VERSION}" \
  "com.facebook.presto:presto-jdbc:${PRESTO_JDBC_VERSION}"

echo "Notebook dependencies pre-fetched into the coursier cache."
