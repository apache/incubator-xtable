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
## Start the demo containers in the background. Builds the XTable jars first
## (via build_demo.sh) if they are not present. Use stop_demo.sh to stop the
## demo when you are done.

set -e

CURRENT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"

## Validate the demo jars are built; build them if anything is missing.
if [ ! -f "${CURRENT_DIR}/jars/versions.properties" ] || ! ls "${CURRENT_DIR}"/jars/xtable-core_*.jar > /dev/null 2>&1; then
  echo "XTable demo jars not found. Building the XTable jars first..."
  "${CURRENT_DIR}/build_demo.sh"
fi

if [ ! -f "${CURRENT_DIR}/jars/versions.properties" ] || ! ls "${CURRENT_DIR}"/jars/xtable-core_*.jar > /dev/null 2>&1; then
  echo "ERROR: the XTable build did not produce the expected demo jars under demo/jars." >&2
  echo "Fix the build (see output above) and re-run this script." >&2
  exit 1
fi

cd "${CURRENT_DIR}"
## The first start builds the notebook image (JDK 11 and all notebook
## dependencies are baked in); later starts reuse the cached image.
docker-compose up -d --build

echo "Waiting for the Jupyter server to start..."
JUPYTER_READY=""
for _ in $(seq 1 150); do
  if docker logs jupyter 2>&1 | grep -qE "Jupyter Server .* is running at"; then
    JUPYTER_READY="yes"
    break
  fi
  sleep 2
done

if [ -n "${JUPYTER_READY}" ]; then
  echo ""
  echo "Jupyter is running at: http://localhost:8888/lab"
  echo "The demo notebooks are under the work/ directory."
else
  echo "Jupyter did not report as running yet; check its status with: docker logs jupyter"
fi
echo "Run ./stop_demo.sh when you are done (add --reset-data to restore the seed datasets)."
