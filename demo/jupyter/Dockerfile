#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# Jupyter image for the XTable demo, based on the almond Scala kernel image.
# Everything the notebooks need at runtime is baked in at build time so the
# first cell does not spend minutes provisioning:
#   - OpenJDK 11 for the Scala kernels (Hudi 1.x jars require Java 11+)
#   - the Scala kernels re-pointed at that JDK
#   - all notebook dependencies (Spark, Hudi, Delta, Iceberg, JDBC drivers)
#     pre-fetched into the coursier cache
FROM almondsh/almond:latest

# Install a JDK 11 for the Scala kernels; the base image only ships Java 8.
RUN mamba create -y -q -n jdk11 -c conda-forge openjdk=11 && mamba clean -afy

# Point all Scala kernels at the JDK 11.
RUN python3 -c "import glob, json; \
paths = glob.glob('/home/jovyan/.local/share/jupyter/kernels/scala*/kernel.json'); \
kernels = [(p, json.load(open(p))) for p in paths]; \
[(k['argv'].__setitem__(0, '/opt/conda/envs/jdk11/bin/java'), k.setdefault('env', {}).__setitem__('JAVA_HOME', '/opt/conda/envs/jdk11'), json.dump(k, open(p, 'w'), indent=2)) for p, k in kernels]; \
print('patched kernels:', [p for p, _ in kernels])"

# Pre-fetch the notebook dependencies into the coursier cache. The versions
# come from demo/jars/versions.properties, generated from the root pom by
# build_demo.sh, so the image always matches what xtable is built against.
COPY jars/versions.properties /tmp/versions.properties
COPY jupyter/prefetch_dependencies.sh /tmp/prefetch_dependencies.sh
RUN bash /tmp/prefetch_dependencies.sh /tmp/versions.properties
