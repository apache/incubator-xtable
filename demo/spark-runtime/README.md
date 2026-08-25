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

This demo shows what the `xtable-spark-runtime` jar is for: a Spark job that already writes a table
adds the jar with `--jars` and calls `XTableSyncService` right after its write, making the table
interoperable without rewriting any data.

It runs the sync in both directions and verifies the row counts, so it also serves as a smoke test
for a release.

| Direction | The job writes | XTable adds |
| --- | --- | --- |
| 1 | `HUDI` | `ICEBERG`, `DELTA` |
| 2 | `ICEBERG` | `HUDI`, `DELTA` |

No Docker and no Scala compiler are needed — `spark-shell` compiles the script for you. This is
separate from the notebook demo in the parent directory.

## Prerequisites

- A Spark **3.4.x** distribution. Hudi 0.14.0 publishes no Spark 3.5 bundle, so the Hudi Spark
  datasource that direction 1 writes with needs a 3.4 runtime. The runtime jar itself supports both
  Spark 3.4 and 3.5.
- JDK 11.
- Maven, to resolve the engine jars in step 2.

Set `SPARK_HOME` and work in a scratch directory:

```shell
export SPARK_HOME=/path/to/spark-3.4.x-bin-hadoop3
mkdir -p /tmp/xtable-spark-demo && cd /tmp/xtable-spark-demo
```

## 1. Get the runtime jar from Maven Central

```shell
BASE=https://repo1.maven.org/maven2/org/apache/xtable/xtable-spark-runtime_2.12/0.4.0-incubating
curl -O $BASE/xtable-spark-runtime_2.12-0.4.0-incubating.jar
curl -O $BASE/xtable-spark-runtime_2.12-0.4.0-incubating.pom
```

The jar is about 4 MB. It holds XTable code only, because it declares Spark, Hadoop, Hudi, Iceberg
and Delta as `provided`.

## 2. Build the engine classpath

A real cluster already supplies the engines. A local Spark distribution does not, so resolve them
from the POM you just downloaded:

```shell
mvn -q -f xtable-spark-runtime_2.12-0.4.0-incubating.pom dependency:build-classpath \
  -DincludeScope=provided -Dmdep.outputFile=engine-classpath-raw.txt
```

Keep the engine jars only. Spark and Hadoop come from the distribution:

```shell
tr ':' '\n' < engine-classpath-raw.txt \
  | grep -E '/(hudi-|iceberg-|delta-|avro-|parquet-|jol-core)' \
  | grep -v 'avro-mapred\|avro-ipc' \
  | paste -sd: - > engine-classpath.txt
```

Keep every `parquet-*` jar, `parquet-format-structures` included. If you drop it, the older copy in
the distribution wins and the read fails with `NoSuchFieldError: size_statistics`.

Direction 2 writes an Iceberg table from Spark, so add the Iceberg Spark runtime as well:

```shell
curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.9.2/iceberg-spark-runtime-3.4_2.12-1.9.2.jar
echo "$PWD/iceberg-spark-runtime-3.4_2.12-1.9.2.jar:$(cat engine-classpath.txt)" > engine-classpath.txt
```

## 3. Run

The engines go on the flat classpath with `extraClassPath`, the way a cluster supplies them. The
runtime jar goes on `--jars`, the way you add it to a job you already run.

```shell
export SPARK_LOCAL_IP=127.0.0.1
export XT_DEMO_DIR=$PWD/out

$SPARK_HOME/bin/spark-shell \
  --master 'local[4]' \
  --driver-memory 4g \
  --conf spark.driver.extraClassPath="$(cat engine-classpath.txt)" \
  --conf spark.executor.extraClassPath="$(cat engine-classpath.txt)" \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalog.xt=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.xt.type=hadoop \
  --conf spark.sql.catalog.xt.warehouse=$XT_DEMO_DIR/iceberg_warehouse \
  --jars $PWD/xtable-spark-runtime_2.12-0.4.0-incubating.jar \
  -i /path/to/incubator-xtable/demo/spark-runtime/XTableInJobSyncDemo.scala
```

Set `SPARK_HOME` explicitly. `spark-submit` and `spark-shell` read it from the environment, so
calling the script by path is not enough when another Spark is already exported. On a Spark 3.5
runtime, the Hudi write in direction 1 fails with
`ClassNotFoundException: org.apache.spark.sql.adapter.Spark2Adapter`.

## Expected output

```
==============================================================================
  DIRECTION 1: the job writes HUDI -> interoperable with ICEBERG and DELTA
==============================================================================
  wrote the Hudi table at /tmp/xtable-spark-demo/out/hudi_orders
  XTableSyncService returned [ICEBERG, DELTA]
  [PASS] hudi -> iceberg        1000 rows
  [PASS] hudi -> delta          1000 rows

==============================================================================
  DIRECTION 2: the job writes ICEBERG -> interoperable with HUDI and DELTA
==============================================================================
  wrote the Iceberg table at /tmp/xtable-spark-demo/out/iceberg_warehouse/db/orders
  XTableSyncService returned [HUDI, DELTA]
  [PASS] iceberg -> hudi        1000 rows
  [PASS] iceberg -> delta       1000 rows

==============================================================================
  RESULT
==============================================================================
  ALL CHECKS PASSED
```

Set `XT_DEMO_ROWS` to change the row count. The default is 1000.

## Notes

- **A Hudi target needs `hoodie.metadata.enable=true` on read.** XTable records a Hudi target's
  file listing in the Hudi metadata table, so without this option the sync reports success and the
  read comes back empty. See [Apache Spark](https://xtable.apache.org/docs/spark) for the read
  options each format needs.
- For paths on S3, add S3A and credentials:

  ```shell
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider
  ```

See [Run an XTable sync on Apache Spark](https://xtable.apache.org/docs/how-to-spark-runtime) for the full guide.
