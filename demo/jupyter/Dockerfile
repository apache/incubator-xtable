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
#   - all notebook dependencies (Spark, Hudi, Delta, Iceberg, JDBC drivers)
#     pre-fetched into the coursier cache
#   - the Scala kernels re-pointed at that JDK, with a bounded heap and a
#     predef that puts every dependency on the classpath at kernel startup
# Pinned by tag and digest. The kernel-patching step at the bottom of this file
# depends on the base image's layout - the kernel path, argv[0] being "java" and
# -Xmx4g being safe to insert at argv[1] - so a moving :latest could break the
# demo silently at build time. 0.14.1 is the image :latest resolved to when this
# was pinned, so behaviour is unchanged; bump both tag and digest together.
FROM almondsh/almond:0.14.1@sha256:0407359be58529eaa8e35bcf4586c3a890123be64005600b381de88fdc3c5de7

# Install a JDK 11 for the Scala kernels; the base image only ships Java 8.
RUN mamba create -y -q -n jdk11 -c conda-forge openjdk=11 && mamba clean -afy

# Pre-fetch the notebook dependencies into the coursier cache. The versions
# come from demo/jars/versions.properties, generated from the root pom by
# build_demo.sh, so the image always matches what xtable is built against.
COPY jars/versions.properties /usr/local/share/xtable/versions.properties
COPY jupyter/prefetch_dependencies.sh /usr/local/share/xtable/prefetch_dependencies.sh
RUN bash /usr/local/share/xtable/prefetch_dependencies.sh /usr/local/share/xtable/versions.properties

# Startup gate: re-verify (and complete, if versions changed since the image
# was built) the dependency cache before the Jupyter server is started.
COPY jupyter/verify_dependencies.sh /usr/local/bin/before-notebook.d/10-xtable-verify-dependencies.sh

# The kernel predef loads all notebook dependencies at kernel startup, so the
# notebooks contain no dependency management code.
COPY jupyter/predef.sc /usr/local/share/xtable/predef.sc

# Point all Scala kernels at the JDK 11 with a bounded heap (local Spark for
# the demo fits comfortably in 4g and this keeps the kernel JVM from starving
# the other demo containers) and wire in the predef.
#
# The assertions matter: without them a base image whose kernel path or argv
# shape changed would make the glob match nothing, the comprehension iterate
# over an empty list, and the build succeed with an unpatched image that fails
# only when a notebook is run.
RUN python3 -c "import glob, json; \
paths = sorted(glob.glob('/home/jovyan/.local/share/jupyter/kernels/scala*/kernel.json')); \
assert paths, 'no scala kernels found under /home/jovyan/.local/share/jupyter/kernels'; \
kernels = [(p, json.load(open(p))) for p in paths]; \
assert all(k['argv'][0] == 'java' for _, k in kernels), 'kernel argv[0] is not java: %r' % [k['argv'][0] for _, k in kernels]; \
[(k['argv'].__setitem__(0, '/opt/conda/envs/jdk11/bin/java'), k['argv'].insert(1, '-Xmx4g'), k['argv'].extend(['--predef', '/usr/local/share/xtable/predef.sc']), k.setdefault('env', {}).__setitem__('JAVA_HOME', '/opt/conda/envs/jdk11'), json.dump(k, open(p, 'w'), indent=2)) for p, k in kernels]; \
print('patched kernels:', [p for p, _ in kernels])"
