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
## Stop the demo containers started by start_demo.sh. Pass --reset-data to also
## restore the seed datasets under demo/data to their original state so the
## demo notebook can be re-run from scratch.

CURRENT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
XTABLE_HOME="$( cd "$(dirname "$CURRENT_DIR")" ; pwd -P )"

cd "$CURRENT_DIR"
docker-compose down

if [ "$1" = "--reset-data" ]; then
  echo "Resetting demo datasets under demo/data..."
  cd "$XTABLE_HOME"
  git checkout -- demo/data
  git clean -fdxq demo/data
fi
