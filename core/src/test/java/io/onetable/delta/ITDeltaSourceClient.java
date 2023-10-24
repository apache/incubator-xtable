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
 
package io.onetable.delta;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.onetable.client.PerTableConfig;
import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.schema.SchemaCatalog;
import io.onetable.model.schema.SchemaVersion;
import io.onetable.model.storage.DataLayoutStrategy;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.TableFormat;

/**
 * A suite of functional tests that assert that the metadata for the Delta table is properly read
 * from disk and converted into OneTable internal representation.
 */
class ITDeltaSourceClient {

  private static SparkSession sparkSession;

  @TempDir public static java.nio.file.Path tempDir;

  private DeltaSourceClientProvider clientProvider;

  @BeforeAll
  public static void setupOnce() {
    sparkSession = buildSparkSession();
  }

  @AfterAll
  public static void teardown() {
    sparkSession.close();
  }

  @BeforeEach
  void setUp() {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", "file:///");

    clientProvider = new DeltaSourceClientProvider();
    clientProvider.init(hadoopConf, null);
  }

  @Test
  void getCurrentSnapshotNonPartitionedTest() {
    // Table name
    final String tableName = getTableName();
    final Path basePath = tempDir.resolve(tableName);
    // Create table with a single row using Spark
    sparkSession.sql(
        "CREATE TABLE `"
            + tableName
            + "` USING DELTA LOCATION '"
            + basePath
            + "' AS SELECT * FROM VALUES (1, 2)");
    // Create Delta source client
    PerTableConfig tableConfig =
        PerTableConfig.builder()
            .tableName(tableName)
            .tableBasePath(basePath.toString())
            .targetTableFormats(Collections.singletonList(TableFormat.ICEBERG))
            .build();
    DeltaSourceClient client = clientProvider.getSourceClientInstance(tableConfig);
    // Get current snapshot
    OneSnapshot snapshot = client.getCurrentSnapshot();
    // Validate table
    validateTable(
        snapshot.getTable(),
        tableName,
        TableFormat.DELTA,
        OneSchema.builder()
            .name("struct")
            .dataType(OneType.RECORD)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("col1")
                        .schema(
                            OneSchema.builder()
                                .name("integer")
                                .dataType(OneType.INT)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("col2")
                        .schema(
                            OneSchema.builder()
                                .name("integer")
                                .dataType(OneType.INT)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build(),
        DataLayoutStrategy.FLAT,
        "file:" + basePath,
        Collections.emptyList());
    // Validate schema catalog
    SchemaCatalog oneSchemaCatalog = snapshot.getSchemaCatalog();
    validateSchemaCatalog(
        oneSchemaCatalog,
        Collections.singletonMap(new SchemaVersion(1, ""), snapshot.getTable().getReadSchema()));
    // TODO: Validate data files (see https://github.com/onetable-io/onetable/issues/96)
  }

  @Test
  void getCurrentSnapshotPartitionedTest() {
    // Table name
    final String tableName = getTableName();
    final Path basePath = tempDir.resolve(tableName);
    // Create table with a single row using Spark
    sparkSession.sql(
        "CREATE TABLE `"
            + tableName
            + "` USING DELTA PARTITIONED BY (part_col)\n"
            + "LOCATION '"
            + basePath
            + "' AS SELECT 'SingleValue' AS part_col, 1 AS col1, 2 AS col2");
    // Create Delta source client
    PerTableConfig tableConfig =
        PerTableConfig.builder()
            .tableName(tableName)
            .tableBasePath(basePath.toString())
            .targetTableFormats(Collections.singletonList(TableFormat.ICEBERG))
            .build();
    DeltaSourceClient client = clientProvider.getSourceClientInstance(tableConfig);
    // Get current snapshot
    OneSnapshot snapshot = client.getCurrentSnapshot();
    // Validate table
    OneField partCol =
        OneField.builder()
            .name("part_col")
            .schema(
                OneSchema.builder()
                    .name("string")
                    .dataType(OneType.STRING)
                    .isNullable(true)
                    .build())
            .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
            .build();
    validateTable(
        snapshot.getTable(),
        tableName,
        TableFormat.DELTA,
        OneSchema.builder()
            .name("struct")
            .dataType(OneType.RECORD)
            .fields(
                Arrays.asList(
                    partCol,
                    OneField.builder()
                        .name("col1")
                        .schema(
                            OneSchema.builder()
                                .name("integer")
                                .dataType(OneType.INT)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("col2")
                        .schema(
                            OneSchema.builder()
                                .name("integer")
                                .dataType(OneType.INT)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build(),
        DataLayoutStrategy.DIR_HIERARCHY_PARTITION_VALUES,
        "file:" + basePath,
        Collections.singletonList(
            OnePartitionField.builder()
                .sourceField(partCol)
                .transformType(PartitionTransformType.VALUE)
                .build()));
    // Validate schema catalog
    SchemaCatalog oneSchemaCatalog = snapshot.getSchemaCatalog();
    validateSchemaCatalog(
        oneSchemaCatalog,
        Collections.singletonMap(new SchemaVersion(1, ""), snapshot.getTable().getReadSchema()));
    // TODO: Validate data files (see https://github.com/onetable-io/onetable/issues/96)
  }

  private static String getTableName() {
    return "test_" + UUID.randomUUID().toString().replace("-", "_");
  }

  @Disabled("Requires Spark 3.4.0+")
  @Test
  void getCurrentSnapshotGenColPartitionedTest() {
    // Table name
    final String tableName = getTableName();
    final Path basePath = tempDir.resolve(tableName);
    // Create table with a single row using Spark
    sparkSession.sql(
        "CREATE TABLE `"
            + tableName
            + "` (id BIGINT, event_time TIMESTAMP, day INT GENERATED ALWAYS AS (DATE_FORMAT(event_time, 'YYYY-MM-dd')))"
            + " USING DELTA LOCATION '"
            + basePath
            + "'");
    sparkSession.sql(
        "INSERT INTO TABLE `"
            + tableName
            + "` VALUES(1, CAST('2012-02-12 00:12:34' AS TIMESTAMP))");
    // Create Delta source client
    PerTableConfig tableConfig =
        PerTableConfig.builder()
            .tableName(tableName)
            .tableBasePath(basePath.toString())
            .targetTableFormats(Collections.singletonList(TableFormat.ICEBERG))
            .build();
    DeltaSourceClient client = clientProvider.getSourceClientInstance(tableConfig);
    // Get current snapshot
    OneSnapshot snapshot = client.getCurrentSnapshot();
    // TODO: Complete and enable test (see https://github.com/onetable-io/onetable/issues/90)
  }

  private static void validateTable(
      OneTable oneTable,
      String tableName,
      TableFormat tableFormat,
      OneSchema readSchema,
      DataLayoutStrategy dataLayoutStrategy,
      String basePath,
      List<OnePartitionField> partitioningFields) {
    Assertions.assertEquals(tableName, oneTable.getName());
    Assertions.assertEquals(tableFormat, oneTable.getTableFormat());
    Assertions.assertEquals(readSchema, oneTable.getReadSchema());
    Assertions.assertEquals(dataLayoutStrategy, oneTable.getLayoutStrategy());
    Assertions.assertEquals(basePath, oneTable.getBasePath());
    Assertions.assertEquals(partitioningFields, oneTable.getPartitioningFields());
  }

  private void validateSchemaCatalog(
      SchemaCatalog oneSchemaCatalog, Map<SchemaVersion, OneSchema> schemas) {
    Assertions.assertEquals(schemas, oneSchemaCatalog.getSchemas());
  }

  private void validateDataFiles(OneDataFiles oneDataFiles, List<OneDataFile> files) {
    Assertions.assertEquals(files, oneDataFiles.getFiles());
  }

  private static SparkSession buildSparkSession() {
    SparkConf sparkConf =
        new SparkConf()
            .setAppName("testdeltasourceclient")
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .set("spark.master", "local[2]")
            .set("spark.sql.shuffle.partitions", "1")
            .set("spark.default.parallelism", "1");
    return SparkSession.builder().config(sparkConf).getOrCreate();
  }
}
