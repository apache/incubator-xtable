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
docker-compose up -d

## Wait for the Jupyter server and print the notebook URL. The first start can
## take a few minutes while the container installs a JDK 11 for the kernels.
echo "Waiting for the Jupyter server to start (the first start can take a few minutes)..."
JUPYTER_URL=""
for _ in $(seq 1 150); do
  JUPYTER_URL=$(docker logs jupyter 2>&1 | grep -o 'http://127.0.0.1:8888/lab?token=[a-zA-Z0-9]*' | tail -1)
  if [ -n "${JUPYTER_URL}" ]; then
    break
  fi
  sleep 2
done

if [ -n "${JUPYTER_URL}" ]; then
  echo ""
  echo "Jupyter is running at: ${JUPYTER_URL}"
  echo "The demo notebooks are under the work/ directory."
else
  echo "Jupyter did not report a URL yet; check its status with: docker logs jupyter"
fi
echo "Run ./stop_demo.sh when you are done (add --reset-data to restore the seed datasets)."
