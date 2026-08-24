/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Demonstrates keeping a table readable in other formats from inside the Spark job that
 * already writes it, using the xtable-spark-runtime bundle. See
 * https://xtable.apache.org/docs/spark-runtime for the accompanying guide, and
 * demo/spark-runtime/README.md for how to build and run this.
 *
 * Both directions are exercised and asserted:
 *   1. a job that writes HUDI, kept readable as ICEBERG and DELTA
 *   2. a job that writes ICEBERG, kept readable as HUDI and DELTA
 *
 * Schema and sample rows are taken from an existing Hudi table so the shapes are realistic.
 *
 * Usage:
 *   spark-submit --class XTableInJobSyncDemo ... xtable-injob-demo.jar \
 *     <source-hudi-table> <output-prefix> [sampleRows]
 */

import java.util.{Arrays => JArrays}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{lit, monotonically_increasing_id}

import org.apache.xtable.spark.{TableSyncSpec, XTableSyncService}

object XTableInJobSyncDemo {

  /** Hudi's own bookkeeping columns; dropped so the sample reads as plain user data. */
  private val HudiMetaColumns = Seq(
    "_hoodie_commit_time",
    "_hoodie_commit_seqno",
    "_hoodie_record_key",
    "_hoodie_partition_path",
    "_hoodie_file_name")

  private var failures = List.empty[String]

  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "usage: <source-hudi-table> <output-prefix> [sampleRows]")
    val sourceHudiTable = args(0).stripSuffix("/")
    val outPrefix = args(1).stripSuffix("/")
    val sampleRows = if (args.length > 2) args(2).toInt else 1000

    val spark = SparkSession
      .builder()
      .appName("xtable-in-job-sync-demo")
      // path-based Iceberg writes for direction 2
      .config("spark.sql.catalog.xt", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.xt.type", "hadoop")
      .config("spark.sql.catalog.xt.warehouse", s"$outPrefix/iceberg_warehouse")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val hadoopConf: Configuration = spark.sparkContext.hadoopConfiguration

    banner("Reading schema and sample rows from the existing Hudi table")
    println(s"  source : $sourceHudiTable")
    val source = spark.read.format("hudi").load(sourceHudiTable)
    println("  schema :")
    source.printSchema()

    val sample = source
      .drop(HudiMetaColumns: _*)
      .limit(sampleRows)
      .withColumn("_xt_key", monotonically_increasing_id())
      .withColumn("_xt_ts", lit(System.currentTimeMillis()))
      .cache()
    val expected = sample.count()
    println(s"  sampled $expected rows, ${sample.schema.fields.length} columns")

    directionHudiSource(spark, hadoopConf, sample, expected, outPrefix)
    directionIcebergSource(spark, hadoopConf, sample, expected, outPrefix)

    banner("RESULT")
    if (failures.isEmpty) {
      println("  ALL CHECKS PASSED")
    } else {
      println(s"  ${failures.size} CHECK(S) FAILED")
      failures.reverse.foreach(f => println(s"    - $f"))
    }
    spark.stop()
    if (failures.nonEmpty) System.exit(1)
  }

  /** Direction 1: the job writes Hudi; XTable adds Iceberg and Delta metadata in the same job. */
  private def directionHudiSource(
      spark: SparkSession,
      hadoopConf: Configuration,
      sample: DataFrame,
      expected: Long,
      outPrefix: String): Unit = {

    banner("DIRECTION 1: job writes HUDI -> readable as ICEBERG and DELTA")
    val basePath = s"$outPrefix/hudi_orders"

    // ---- the write the job already does -------------------------------------------------
    sample.write
      .format("hudi")
      .option("hoodie.table.name", "orders")
      .option("hoodie.datasource.write.recordkey.field", "_xt_key")
      .option("hoodie.datasource.write.precombine.field", "_xt_ts")
      .option("hoodie.datasource.write.operation", "bulk_insert")
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.metadata.enable", "false")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    println(s"  wrote Hudi table at $basePath")

    // ---- the only addition the job needs -------------------------------------------------
    val results = new XTableSyncService().sync(
      TableSyncSpec
        .builder()
        .key("orders")
        .basePath(basePath)
        .sourceFormat("HUDI")
        .targets(JArrays.asList("ICEBERG", "DELTA"))
        .build(),
      hadoopConf)
    println(s"  XTableSyncService returned: ${results.keySet()}")

    check("hudi -> iceberg", expected, readCount(spark, "iceberg", basePath))
    check("hudi -> delta", expected, readCount(spark, "delta", basePath))
  }

  /** Direction 2: the job writes Iceberg; XTable adds Hudi and Delta metadata in the same job. */
  private def directionIcebergSource(
      spark: SparkSession,
      hadoopConf: Configuration,
      sample: DataFrame,
      expected: Long,
      outPrefix: String): Unit = {

    banner("DIRECTION 2: job writes ICEBERG -> readable as HUDI and DELTA")
    val basePath = s"$outPrefix/iceberg_warehouse/db/orders"

    // ---- the write the job already does -------------------------------------------------
    spark.sql("CREATE NAMESPACE IF NOT EXISTS xt.db")
    spark.sql("DROP TABLE IF EXISTS xt.db.orders")
    sample.writeTo("xt.db.orders").create()
    println(s"  wrote Iceberg table at $basePath")

    // ---- the only addition the job needs -------------------------------------------------
    // An Iceberg table keeps its data files under <basePath>/data, and the targets write their
    // metadata alongside those files, so dataPath is set as well.
    val results = new XTableSyncService().sync(
      TableSyncSpec
        .builder()
        .key("orders")
        .basePath(basePath)
        .dataPath(s"$basePath/data")
        .sourceFormat("ICEBERG")
        .targets(JArrays.asList("HUDI", "DELTA"))
        .build(),
      hadoopConf)
    println(s"  XTableSyncService returned: ${results.keySet()}")

    check("iceberg -> hudi", expected, readCount(spark, "hudi", s"$basePath/data"))
    check("iceberg -> delta", expected, readCount(spark, "delta", s"$basePath/data"))
  }

  /**
   * Read options a cross-engine target needs, mirroring ITXTableSparkRuntimeBundle#readOptions.
   * A Hudi target XTable wrote records its file listing in the Hudi metadata table, so the reader
   * must have that enabled or it sees an empty table. Iceberg's vectorized Arrow reader mis-casts
   * timestamps on tables whose files another engine wrote.
   */
  private def readOptions(format: String): Map[String, String] = format.toUpperCase match {
    case "HUDI" =>
      Map(
        "hoodie.metadata.enable" -> "true",
        "hoodie.datasource.read.extract.partition.values.from.path" -> "true")
    case "ICEBERG" => Map("vectorization-enabled" -> "false")
    case _ => Map.empty
  }

  private def readCount(spark: SparkSession, format: String, path: String): Either[String, Long] =
    try Right(spark.read.options(readOptions(format)).format(format).load(path).count())
    catch { case t: Throwable => Left(s"${t.getClass.getSimpleName}: ${t.getMessage}") }

  private def check(label: String, expected: Long, actual: Either[String, Long]): Unit =
    actual match {
      case Right(n) if n == expected => println(f"  [PASS] $label%-22s $n rows")
      case Right(n) =>
        val msg = s"$label: expected $expected rows, read $n"
        println(s"  [FAIL] $msg"); failures ::= msg
      case Left(err) =>
        val msg = s"$label: read failed: $err"
        println(s"  [FAIL] $msg"); failures ::= msg
    }

  private def banner(s: String): Unit = {
    println()
    println("=" * 78)
    println(s"  $s")
    println("=" * 78)
  }
}
