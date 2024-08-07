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

# syntax=docker/dockerfile:1

FROM maven:3.9.8-eclipse-temurin-11-focal as package

WORKDIR /build

COPY ./ ./
RUN --mount=type=cache,target=/root/.m2 \
    MAVEN_OPTS=-Dorg.slf4j.simpleLogger.defaultLogLevel=warn mvn  -B  package -DskipTests 
RUN mv xtable-utilities/target/xtable-utilities-$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)-bundled.jar target/app.jar

FROM eclipse-temurin:17-jre-jammy AS final

# Create a non-privileged user that the app will run under.
# See https://docs.docker.com/go/dockerfile-user-best-practices/
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser
USER appuser

# Copy the executable from the "package" stage.
COPY --from=package build/target/app.jar ./app.jar

# It's necessary to added opens to run spark on java >= 17
# https://stackoverflow.com/questions/73465937/apache-spark-3-3-0-breaks-on-java-17-with-cannot-access-class-sun-nio-ch-direct
ENTRYPOINT [ 
            "java", \
            "--add-opens=java.base/sun.nio.hb=ALL-UNNAMED", \
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", \      
            "--add-opens=java.base/java.nio=ALL-UNNAMED", \     
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED", \
            "--add-opens=java.base/java.util=ALL-UNNAMED", \
            "--add-opens=java.base/java.lang=ALL-UNNAMED", \
            "-jar", \
            "./app.jar" \ 
        ]
