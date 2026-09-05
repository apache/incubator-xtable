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
## Startup gate for the Jupyter container: verifies that every notebook
## dependency is present in the local coursier cache (downloading anything
## missing, e.g. after a version bump without an image rebuild) BEFORE the
## Jupyter server starts. The image build pre-fetches everything, so when the
## versions are unchanged this completes in a few seconds without any network
## access.

PROPS_FILE=/home/jars/versions.properties
if [ ! -f "${PROPS_FILE}" ]; then
  PROPS_FILE=/usr/local/share/xtable/versions.properties
fi

echo "xtable-demo: verifying notebook dependencies before starting Jupyter (using ${PROPS_FILE})..."
if ! bash /usr/local/share/xtable/prefetch_dependencies.sh "${PROPS_FILE}"; then
  echo "xtable-demo: ERROR - failed to resolve the notebook dependencies; not starting Jupyter." >&2
  exit 1
fi
echo "xtable-demo: all notebook dependencies are available, starting Jupyter."
