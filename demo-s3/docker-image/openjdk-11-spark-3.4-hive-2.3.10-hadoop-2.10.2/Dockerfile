# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ARG ARCH
FROM eclipse-temurin:11-jre-focal

ARG SPARK_VERSION=3.4.3
ARG HADOOP_VERSION=3
ARG HUDI_VERSION=0.14.1
ARG SCALA_VERSION=2.12

ARG SPARK_ARTIFACT="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"

ENV SPARK_HOME=/spark
ENV HADOOP_HOME=/hadoop
ENV HIVE_HOME=/hive

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip git vim; \
    rm -rf /var/lib/apt/lists/*

RUN set -xeu; \
    wget -nv "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_ARTIFACT}.tgz"; \
    tar -xf ${SPARK_ARTIFACT}.tgz; \
    rm ${SPARK_ARTIFACT}.tgz; \
    ln -sn /${SPARK_ARTIFACT} ${SPARK_HOME}

RUN set -xeu; \
    wget -nv "https://archive.apache.org/dist/hive/hive-2.3.10/apache-hive-2.3.10-bin.tar.gz"; \
    tar -xf apache-hive-2.3.10-bin.tar.gz; \
    rm apache-hive-2.3.10-bin.tar.gz; \
    ln -sn /apache-hive-2.3.10-bin /hive

RUN set -xeu; \
    wget -nv "https://dlcdn.apache.org/hadoop/common/hadoop-2.10.2/hadoop-2.10.2.tar.gz"; \
    tar -xf hadoop-2.10.2.tar.gz; \
    rm hadoop-2.10.2.tar.gz; \ 
    ln -sn /hadoop-2.10.2 /hadoop

WORKDIR ${SPARK_HOME}/bin

# Create Hive user to match Hive container
RUN adduser hive

ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN cd /opt && git clone https://github.com/apache/hudi.git 
RUN /spark/bin/pyspark --packages com.amazonaws:aws-java-sdk-s3:1.12.661,org.apache.hadoop:hadoop-aws:3.3.1

EXPOSE 10213

CMD spark-submit \
   --master "local[*]" \
   --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
   --name "Thrift JDBC/ODBC Server" \
   --conf spark.hive.server2.thrift.port=10213 \
   spark-internal
