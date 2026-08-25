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
 * Keeps a table interoperable from inside the Spark job that writes it.
 * The job adds the xtable-spark-runtime jar with --jars and calls XTableSyncService after
 * the write. No data is rewritten.
 *
 * The script runs both directions and checks the row counts:
 *   1. the job writes HUDI    -> XTable adds ICEBERG and DELTA
 *   2. the job writes ICEBERG -> XTable adds HUDI and DELTA
 *
 * See demo/spark-runtime/README.md for how to run it, and
 * https://xtable.apache.org/docs/how-to-spark-runtime for the guide.
 *
 * Environment:
 *   XT_DEMO_DIR   output directory (default /tmp/xtable-demo)
 *   XT_DEMO_ROWS  number of sample rows (default 1000)
 */

import java.util.{Arrays => JArrays}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, concat, lit}

import org.apache.xtable.spark.{TableSyncSpec, XTableSyncService}

val outPrefix = sys.env.getOrElse("XT_DEMO_DIR", "/tmp/xtable-demo").stripSuffix("/")
val rows = sys.env.getOrElse("XT_DEMO_ROWS", "1000").toInt
val hadoopConf: Configuration = spark.sparkContext.hadoopConfiguration
var failures = List.empty[String]

def banner(s: String): Unit = {
  println()
  println("=" * 78)
  println(s"  $s")
  println("=" * 78)
}

/**
 * Read options a cross-engine target needs. XTable records the file listing of a Hudi target
 * in the Hudi metadata table, so the reader must enable that table or it sees no rows. The
 * vectorized Arrow reader of Iceberg mis-casts timestamps on a table another engine wrote.
 */
def readOptions(format: String): Map[String, String] = format.toUpperCase match {
  case "HUDI" =>
    Map(
      "hoodie.metadata.enable" -> "true",
      "hoodie.datasource.read.extract.partition.values.from.path" -> "true")
  case "ICEBERG" => Map("vectorization-enabled" -> "false")
  case _ => Map.empty
}

def check(label: String, expected: Long, format: String, path: String): Unit = {
  val actual =
    try Right(spark.read.options(readOptions(format)).format(format).load(path).count())
    catch { case t: Throwable => Left(s"${t.getClass.getSimpleName}: ${t.getMessage}") }
  actual match {
    case Right(n) if n == expected =>
      println(f"  [PASS] $label%-22s $n rows")
    case Right(n) =>
      val m = s"$label: expected $expected rows, read $n"
      println(s"  [FAIL] $m")
      failures ::= m
    case Left(e) =>
      val m = s"$label: read failed: $e"
      println(s"  [FAIL] $m")
      failures ::= m
  }
}

banner("Sample data")
val orders: DataFrame = spark
  .range(rows)
  .toDF("order_id")
  .withColumn("customer", concat(lit("cust-"), col("order_id") % 50))
  .withColumn("amount", col("order_id") * 3 + 7)
  .withColumn("ts", lit(System.currentTimeMillis()))
println(s"  $rows rows, ${orders.schema.fields.length} columns")

banner("DIRECTION 1: the job writes HUDI -> interoperable with ICEBERG and DELTA")
val hudiPath = s"$outPrefix/hudi_orders"

// ---- the write the job already does ------------------------------------------------------
orders.write
  .format("hudi")
  .option("hoodie.table.name", "orders")
  .option("hoodie.datasource.write.recordkey.field", "order_id")
  .option("hoodie.datasource.write.precombine.field", "ts")
  .option("hoodie.datasource.write.operation", "bulk_insert")
  .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
  .option("hoodie.metadata.enable", "false")
  .mode(SaveMode.Overwrite)
  .save(hudiPath)
println(s"  wrote the Hudi table at $hudiPath")

// ---- the one call the job adds -----------------------------------------------------------
val hudiResults = new XTableSyncService().sync(
  TableSyncSpec
    .builder()
    .key("orders")
    .basePath(hudiPath)
    .sourceFormat("HUDI")
    .targets(JArrays.asList("ICEBERG", "DELTA"))
    .build(),
  hadoopConf)
println(s"  XTableSyncService returned ${hudiResults.keySet()}")

check("hudi -> iceberg", rows, "iceberg", hudiPath)
check("hudi -> delta", rows, "delta", hudiPath)

banner("DIRECTION 2: the job writes ICEBERG -> interoperable with HUDI and DELTA")
val icebergBase = s"$outPrefix/iceberg_warehouse/db/orders"

// ---- the write the job already does ------------------------------------------------------
spark.sql("CREATE NAMESPACE IF NOT EXISTS xt.db")
spark.sql("DROP TABLE IF EXISTS xt.db.orders")
orders.writeTo("xt.db.orders").create()
println(s"  wrote the Iceberg table at $icebergBase")

// ---- the one call the job adds -----------------------------------------------------------
// An Iceberg table keeps its data files under <basePath>/data. Each target writes its
// metadata next to those files, so the demo sets dataPath as well.
val icebergResults = new XTableSyncService().sync(
  TableSyncSpec
    .builder()
    .key("orders")
    .basePath(icebergBase)
    .dataPath(s"$icebergBase/data")
    .sourceFormat("ICEBERG")
    .targets(JArrays.asList("HUDI", "DELTA"))
    .build(),
  hadoopConf)
println(s"  XTableSyncService returned ${icebergResults.keySet()}")

check("iceberg -> hudi", rows, "hudi", s"$icebergBase/data")
check("iceberg -> delta", rows, "delta", s"$icebergBase/data")

banner("RESULT")
if (failures.isEmpty) {
  println("  ALL CHECKS PASSED")
} else {
  println(s"  ${failures.size} CHECK(S) FAILED")
  failures.reverse.foreach(f => println(s"    - $f"))
}
System.exit(if (failures.isEmpty) 0 else 1)
