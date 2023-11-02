/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package io.onetable.iceberg;

import java.net.URL;
import java.nio.file.Path;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import io.onetable.client.OneTableClient;
import io.onetable.client.PerTableConfig;
import io.onetable.model.storage.*;

@Execution(ExecutionMode.SAME_THREAD)
public class ITIcebergSourceClient {

  // The spark session is used for DML operations on the source (Iceberg) table only. OneTable does
  // not depend on this
  private static SparkSession sparkSession;

  @TempDir private static Path workingDir;

  private OneTableClient oneTableClient;
  private IcebergSourceClientProvider clientProvider;
  private static final Configuration hadoopConf = new Configuration();
  private static final String icebergNs = "iceberg_ns";

  @BeforeAll
  public static void setupOnce() {
    hadoopConf.set("fs.defaultFS", "file:///");
  }

  @BeforeEach
  void setUp() {
    sparkSession =
        SparkSession.builder()
            .appName("TestIcebergTable")
            .master("local[4]")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse", "file://" + workingDir)
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate();

    clientProvider = new IcebergSourceClientProvider();
    clientProvider.init(hadoopConf, null);

    oneTableClient = new OneTableClient(hadoopConf);
  }

  @AfterEach
  public void teardown() {
    if (sparkSession != null) {
      sparkSession.close();
    }
  }

  @Test
  void syncIceToDeltaTest() throws NoSuchTableException {
    List<TableFormat> targetTableFormats = Collections.singletonList(TableFormat.DELTA);
    String tableName = "iceberg_SalesTerritory_" + (int)(Math.random() * 10000);
    createSalesTable(tableName);
    appendRecords("datasets/DimSalesTerritory_part1.csv", tableName);
    appendRecords("datasets/DimSalesTerritory_part2.csv", tableName);
    generateTargets(targetTableFormats, tableName);
  }

  @Test
  void syncIceToHudiTest() throws NoSuchTableException {
    // TODO hudi fails here
    List<TableFormat> targetTableFormats = Collections.singletonList(TableFormat.HUDI);
    String tableName = "iceberg_SalesTerritory_" + (int)(Math.random() * 10000);
    createSalesTable(tableName);
    appendRecords("datasets/DimSalesTerritory_part1.csv", tableName);
    appendRecords("datasets/DimSalesTerritory_part2.csv", tableName);
    generateTargets(targetTableFormats, tableName);
  }

  @Test
  void syncIceToDeltaAndHudiTest() throws NoSuchTableException {
    List<TableFormat> targetTableFormats = Arrays.asList(TableFormat.DELTA, TableFormat.HUDI);
    String tableName = "iceberg_SalesTerritory_" + (int)(Math.random() * 10000);
    createSalesTable(tableName);
    appendRecords("datasets/DimSalesTerritory_part1.csv", tableName);
    appendRecords("datasets/DimSalesTerritory_part2.csv", tableName);
    generateTargets(targetTableFormats, tableName);
  }

  private void generateTargets(List<TableFormat> targetTableFormats, String tableName) {
    Path basePath = workingDir.resolve(icebergNs).resolve(tableName);
    PerTableConfig tableConfig =
        PerTableConfig.builder()
            .tableName(tableName)
            .namespace(new String[] {icebergNs})
            .tableBasePath(basePath.toString())
            .targetTableFormats(targetTableFormats)
            .build();

    oneTableClient.sync(tableConfig, clientProvider);
  }

  private void appendRecords(String records, String tableName) throws NoSuchTableException {
    String tableFQN = icebergNs + "." + tableName;
    DataFrameReader reader = sparkSession.read().option("header", true).option("inferSchema", true);
    URL resource = getClass().getClassLoader().getResource(records);
    Dataset<Row> df = reader.csv(resource.getPath());
    df.writeTo(tableFQN).append();
  }

  private static void createSalesTable(String tableName) {
    String tableFQN = icebergNs + "." + tableName;
    sparkSession.sql(
        " CREATE TABLE "
            + tableFQN
            + "( "
            + "SalesTerritoryKey int, "
            + "GeographyKey int, "
            + "SalesTerritoryName string, "
            + "SalesTerritoryRegion string, "
            + "SalesTerritoryCountry string, "
            + "SalesTerritoryGroup string, "
            + "SalesTerritoryLevel string, "
            + "SalesTerritoryManager int, "
            + "Status string, "
            + "EmployeeKey int "
            + ") "
            + "USING ICEBERG "
            + "PARTITIONED BY (GeographyKey);");
  }
}
