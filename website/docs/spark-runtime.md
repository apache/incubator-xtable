---
title: "Run XTable on Apache Spark"
---

# Running Apache XTable™ (Incubating) on Apache Spark

The `xtable-spark-runtime` module publishes a self-contained (shaded) bundle jar that runs an
XTable metadata sync with `spark-submit` on an existing Apache Spark cluster. It is the
`spark-submit` equivalent of the `RunSync` utility: no data is rewritten, only the target table
format metadata is generated alongside the existing data files.

Use this when you already run Spark (EMR, Dataproc, HDInsight, Databricks, or a local install) and
want to add interoperability without standing up a separate process.

## Build the bundle

From the project root:

```shell md title="shell"
./mvnw clean package -pl xtable-spark-runtime -am -DskipTests
```

This produces the shaded bundle at
`xtable-spark-runtime/target/xtable-spark-runtime_2.12-<version>.jar`.

## Quick start

Sync an existing Hudi table to Iceberg and Delta:

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  --master 'local[*]' \
  xtable-spark-runtime_2.12-<version>.jar \
  --basepath /path/to/hudi_table \
  --sourceformat HUDI \
  --targets ICEBERG,DELTA
```

Sync an Iceberg table to Hudi:

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  xtable-spark-runtime_2.12-<version>.jar \
  --basepath /path/to/iceberg_table \
  --sourceformat ICEBERG \
  --targets HUDI
```

Sync a partitioned Hudi table (pass the source partition spec):

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  xtable-spark-runtime_2.12-<version>.jar \
  --basepath /path/to/hudi_table \
  --sourceformat HUDI \
  --targets DELTA \
  --partitionspec level:VALUE
```

The engine libraries (Hudi, Iceberg, Delta, Avro, Parquet) are `provided` — the bundle expects them
on the Spark runtime classpath, which is the case on a standard Spark install with the relevant
format support.

## Command-line options

| Option | Required | Description |
| --- | --- | --- |
| `--basepath` | yes | Base path of the source table. |
| `--sourceformat` | yes | Source table format: `HUDI`, `ICEBERG`, `DELTA`, `PAIMON`, or `PARQUET`. |
| `--targets` | yes | Comma-separated target formats, e.g. `ICEBERG,DELTA`. |
| `--datapath` | no | Path to the data files if different from the base path (e.g. Iceberg keeps data under `<basePath>/data`). |
| `--tablename` | no | Table name; defaults to the last segment of the base path. |
| `--namespace` | no | Dot-separated table namespace. |
| `--partitionspec` | no | Hudi source partition field spec, e.g. `level:VALUE`. |
| `--usedeltakernel` | no | Force the Spark-free Delta Kernel for the Delta source/target. Auto-enabled on Spark 3.5+ (see below). |
| `--help` | no | Print usage. |

## Supported formats

Paimon and Parquet are read-only sources (there is no corresponding write target).

| Source ↓ / Target → | Hudi | Iceberg | Delta |
| --- | :---: | :---: | :---: |
| **Hudi** | – | ✅ | ✅ |
| **Iceberg** | ✅ | – | ✅ |
| **Delta** | ✅ | ✅ | – |
| **Paimon** | ✅ | ✅ | ✅ |
| **Parquet** | ✅ | ✅ | ✅ |

## Spark version compatibility

Hudi and Iceberg conversion use only Spark-free core classes, so they run on any of the Spark lines
below. Delta is the only Spark-version-sensitive engine, and the bundle picks the right
implementation automatically:

| Spark version | Hudi / Iceberg | Delta implementation |
| --- | :---: | --- |
| 3.4.x | ✅ | Delta Standalone (`delta-core`) |
| 3.5.x and newer | ✅ | Delta Kernel (Spark-free), selected automatically |

On Spark 3.5+, the bundled `delta-core` does not run, so a Delta source or target is routed through
the Spark-free [Delta Kernel](https://docs.delta.io/latest/delta-kernel.html) implementation
automatically — no flag needed. To force Kernel on any Spark version (e.g. Spark 3.4), pass
`--usedeltakernel`.

## Next steps

- See [Installation](/docs/setup) for building the project.
- See the [Quickstart](/docs/how-to) for an end-to-end interoperability walkthrough.
- To query a synced table from Spark, see [Apache Spark](/docs/spark).
