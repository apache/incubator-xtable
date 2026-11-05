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

REPO=$1
VERSION=$2

STAGING_REPO="https://repository.apache.org/content/repositories/${REPO}/org/apache/xtable"

# Every artifact is checked along with its .asc, .md5 and .sha1
declare -a checksums=("" ".asc" ".md5" ".sha1")
declare -a jar_suffixes=(".pom" ".jar" "-sources.jar" "-javadoc.jar")

# xtable-utilities and xtable-spark-extensions are not deployed
declare -a poms=("xtable" "xtable-hudi-support")
declare -a jars=("xtable-api" "xtable-core_2.12" "xtable-hudi-support-utils" "xtable-hudi-support-extensions_2.12"
"xtable-iceberg-pluggable-tf_2.12" "xtable-aws" "xtable-hive-metastore" "xtable-service" "xtable-spark-runtime_2.12")
declare -a extras=("xtable-core_2.12:-tests.jar" "xtable-aws:-bundled.jar" "xtable-hive-metastore:-bundled.jar"
"xtable-hudi-support-extensions_2.12:-bundled.jar")

declare -a files=()
for artifact in "${poms[@]}"; do
  files+=("$artifact/$artifact-${VERSION}.pom")
done
for artifact in "${jars[@]}"; do
  for suffix in "${jar_suffixes[@]}"; do
    files+=("$artifact/$artifact-${VERSION}$suffix")
  done
done
for extra in "${extras[@]}"; do
  artifact=${extra%%:*}
  files+=("$artifact/$artifact-${VERSION}${extra#*:}")
done

missing=0
for file in "${files[@]}"; do
  for checksum in "${checksums[@]}"; do
    url=${STAGING_REPO}/${file%%/*}/${VERSION}/${file#*/}$checksum
    if curl --silent --head --fail --output /dev/null "$url"; then
      echo "Artifact exists: $url"
    else
      echo "Artifact missing: $url"
      missing=1
    fi
  done
done

if [ "$missing" -ne 0 ]; then
  echo "Validation failed: artifacts are missing."
  exit 1
fi
echo "All artifacts exist. Validation succeeds."
