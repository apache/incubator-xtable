<!--
 - Licensed to the Apache Software Foundation (ASF) under one
 - or more contributor license agreements.  See the NOTICE file
 - distributed with this work for additional information
 - regarding copyright ownership.  The ASF licenses this file
 - to you under the Apache License, Version 2.0 (the
 - "License"); you may not use this file except in compliance
 - with the License.  You may obtain a copy of the License at
 -
 -     http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
-->

# In-job sync demo for `xtable-spark-runtime`

`XTableInJobSyncDemo.scala` shows the case the `xtable-spark-runtime` bundle exists for: a Spark
job that already writes a table calls `XTableSyncService` right after its write, so the table
becomes readable in the other formats without a second job to schedule.

It runs both directions and asserts row counts, so it doubles as a smoke test of a release:

| Direction | Source the job writes | Formats XTable adds |
| --- | --- | --- |
| 1 | `HUDI` | `ICEBERG`, `DELTA` |
| 2 | `ICEBERG` | `HUDI`, `DELTA` |

Schema and sample rows come from an existing Hudi table you point it at, so the column shapes are
realistic rather than synthetic.

This is separate from the notebook demo in the parent directory. It needs no Docker.

## Prerequisites

- A Spark **3.4.x** distribution. The bundle's engine set is Spark 3.4 aligned, and Hudi 0.14.0
  publishes no Spark 3.5 bundle, so Spark's Hudi datasource needs a 3.4 runtime here.
- JDK 11.
- A Scala 2.12 compiler.

## Build the engine classpath

The engines are `provided` by design, so supply them from the module's own dependency set:

```shell
./mvnw dependency:build-classpath -pl xtable-spark-runtime \
  -DincludeScope=provided \
  -Dmdep.outputFile=target/engine-classpath.txt
```

Keep only the engine libraries; Spark and Hadoop come from the distribution:

```shell
tr ':' '\n' < xtable-spark-runtime/target/engine-classpath.txt \
  | grep -E '/(hudi-|iceberg-|delta-|avro-1\.12|parquet-.*-1\.15|jol-core)' \
  | grep -v 'avro-mapred\|avro-ipc' \
  | paste -sd: - > engine-classpath.txt
```

Keep every `parquet-*` jar, `parquet-format-structures` included. Dropping it leaves the
distribution's older copy next to `parquet-hadoop`, which fails at read time with
`NoSuchFieldError: size_statistics`.

Direction 2 also needs a Spark-side Iceberg reader, and S3 paths need S3A:

```shell
org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.9.2
org.apache.hadoop:hadoop-aws:<the hadoop version your Spark ships>
com.amazonaws:aws-java-sdk-bundle:<matching version>
```

## Build the demo jar

```shell
scalac -classpath "$(cat engine-classpath.txt):/path/to/xtable-spark-runtime_2.12-0.4.0-incubating.jar:$SPARK_HOME/jars/*" \
  -d classes demo/spark-runtime/XTableInJobSyncDemo.scala
( cd classes && jar cf ../xtable-injob-demo.jar . )
```

## Run

```shell
export SPARK_HOME=/path/to/spark-3.4.x-bin-hadoop3

SPARK_LOCAL_IP=127.0.0.1 $SPARK_HOME/bin/spark-submit \
  --master 'local[4]' \
  --driver-memory 4g \
  --conf spark.driver.extraClassPath="$(cat engine-classpath.txt)" \
  --conf spark.executor.extraClassPath="$(cat engine-classpath.txt)" \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --jars /path/to/xtable-spark-runtime_2.12-0.4.0-incubating.jar \
  --class XTableInJobSyncDemo \
  xtable-injob-demo.jar \
  <existing-hudi-table> <output-prefix> 1000
```

Set `SPARK_HOME` explicitly. `spark-submit` reads it from the environment, so calling the script
by path is not enough if another Spark is already exported. A Spark 3.5 runtime falls through
Hudi 0.14.0's version ladder and fails with
`ClassNotFoundException: org.apache.spark.sql.adapter.Spark2Adapter`.

For paths on S3, add S3A and credentials:

```shell
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider \
```

## Expected output

```
DIRECTION 1: job writes HUDI -> readable as ICEBERG and DELTA
  [PASS] hudi -> iceberg        1000 rows
  [PASS] hudi -> delta          1000 rows
DIRECTION 2: job writes ICEBERG -> readable as HUDI and DELTA
  [PASS] iceberg -> hudi        1000 rows
  [PASS] iceberg -> delta       1000 rows
RESULT
  ALL CHECKS PASSED
```

## Reading a Hudi target back

XTable records a Hudi target's file listing in the Hudi metadata table, so a reader must enable it:

```scala
spark.read
  .option("hoodie.metadata.enable", "true")
  .option("hoodie.datasource.read.extract.partition.values.from.path", "true")
  .format("hudi")
  .load(dataPath)
```

Without `hoodie.metadata.enable`, the sync still reports success and the read returns zero rows.
