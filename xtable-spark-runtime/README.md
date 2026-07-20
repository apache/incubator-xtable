<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# XTable Spark Runtime

`xtable-spark-runtime` publishes a self-contained (shaded, relocated) bundle jar that runs an
Apache XTable™ metadata sync with `spark-submit` on an existing Apache Spark cluster. It is the
`spark-submit` equivalent of the `RunSync` utility — no data is rewritten, only the target table
format metadata is generated alongside the existing data files.

The engine libraries (Hudi, Iceberg, Delta, Avro, Parquet) are `provided`: the user brings their
own engine versions from the Spark runtime, so the thin bundle stays compatible across versions.

## Build

From the project root:

```shell
./mvnw clean package -pl xtable-spark-runtime -am -DskipTests
```

The shaded bundle is written to
`xtable-spark-runtime/target/xtable-spark-runtime_2.12-<version>.jar`.

## Usage

```shell
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  --master 'local[*]' \
  xtable-spark-runtime_2.12-<version>.jar \
  --basepath /path/to/hudi_table \
  --sourceformat HUDI \
  --targets ICEBERG,DELTA
```

| Option | Required | Description |
| --- | --- | --- |
| `--basepath` | yes | Base path of the source table. |
| `--sourceformat` | yes | `HUDI`, `ICEBERG`, `DELTA`, `PAIMON`, or `PARQUET`. |
| `--targets` | yes | Comma-separated target formats, e.g. `ICEBERG,DELTA`. |
| `--datapath` | no | Path to the data files if different from the base path. |
| `--tablename` | no | Table name; defaults to the last segment of the base path. |
| `--namespace` | no | Dot-separated table namespace. |
| `--partitionspec` | no | Hudi source partition field spec, e.g. `level:VALUE`. |
| `--usedeltakernel` | no | Force the Spark-free Delta Kernel for Delta; auto-enabled on Spark 3.5+. |
| `--help` | no | Print usage. |

Paimon and Parquet are read-only sources; targets are Hudi, Iceberg, and Delta.

## Spark version compatibility

Hudi and Iceberg conversion use only Spark-free core classes and run on any Spark line below. Delta
is the only Spark-version-sensitive engine, and the bundle selects the implementation automatically:

| Spark version | Hudi / Iceberg | Delta |
| --- | :---: | --- |
| 3.4.x | ✅ | Delta Standalone (`delta-core`) |
| 3.5.x and newer | ✅ | Delta Kernel (Spark-free), selected automatically |

On Spark 3.5+ the bundled `delta-core` does not run, so a Delta source/target is routed through the
Spark-free Delta Kernel automatically. Pass `--usedeltakernel` to force it on any Spark version.

## Bundle validation IT

`ITXTableSparkRuntimeBundle` `spark-submit`s the shaded jar against a real Spark distribution, one
case per direction across Hudi, Iceberg, and Delta, and asserts the target is data-equivalent to
the source. It self-skips unless `SPARK_HOME` is set, so it does not run in the normal build:

```shell
SPARK_LOCAL_IP=127.0.0.1 SPARK_HOME=/path/to/spark-3.5.x-bin-hadoop3 \
  ./mvnw verify -pl xtable-spark-runtime
```

CI runs this IT on both Spark 3.4 and 3.5 (see
`.github/workflows/spark-runtime-validation.yml`).

For more, see the docs: [Run XTable on Apache Spark](https://xtable.apache.org/docs/spark-runtime).
