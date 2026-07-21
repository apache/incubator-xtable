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
## The almond image ships only Java 8, but Hudi 1.x (required by xtable-core)
## is compiled for Java 11+. Install a JDK 11 and point the Scala kernels at it.
if [ ! -d /opt/conda/envs/jdk11 ]; then
  echo "setup-jdk11: installing OpenJDK 11 (first start only)..."
  mamba create -y -q -n jdk11 -c conda-forge openjdk=11
fi

python3 - <<'PYEOF'
import glob
import json

for path in glob.glob("/home/jovyan/.local/share/jupyter/kernels/scala*/kernel.json"):
    with open(path) as f:
        kernel = json.load(f)
    kernel["argv"][0] = "/opt/conda/envs/jdk11/bin/java"
    kernel.setdefault("env", {})["JAVA_HOME"] = "/opt/conda/envs/jdk11"
    with open(path, "w") as f:
        json.dump(kernel, f, indent=2)
    print(f"setup-jdk11: pointed {path} at JDK 11")
PYEOF
