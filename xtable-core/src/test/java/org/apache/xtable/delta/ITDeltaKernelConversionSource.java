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
 
package org.apache.xtable.delta;

import static org.apache.xtable.testutil.ITTestUtils.validateTable;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.xtable.GenericTable;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.kernel.DeltaKernelConversionSource;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.storage.*;
import org.apache.xtable.model.storage.DataLayoutStrategy;
import org.apache.xtable.model.storage.TableFormat;

public class ITDeltaKernelConversionSource {
  private static final InternalField COL1_INT_FIELD =
      InternalField.builder()
          .name("col1")
          .schema(
              InternalSchema.builder()
                  .name("integer")
                  .dataType(InternalType.INT)
                  .isNullable(true)
                  .build())
          .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
          .build();

  private static final InternalField COL2_INT_FIELD =
      InternalField.builder()
          .name("col2")
          .schema(
              InternalSchema.builder()
                  .name("integer")
                  .dataType(InternalType.INT)
                  .isNullable(true)
                  .build())
          .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
          .build();

  private static final InternalField COL3_STR_FIELD =
      InternalField.builder()
          .name("col3")
          .schema(
              InternalSchema.builder()
                  .name("string")
                  .dataType(InternalType.STRING)
                  .isNullable(true)
                  .build())
          .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
          .build();

  private DeltaKernelConversionSourceProvider conversionSourceProvider;
  private static SparkSession sparkSession;

  @BeforeAll
  public static void setupOnce() {
    sparkSession =
        SparkSession.builder()
            .appName("TestDeltaTable")
            .master("local[4]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.serializer", KryoSerializer.class.getName())
            .getOrCreate();
  }

  @TempDir private static Path tempDir;

  @BeforeEach
  void setUp() {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", "file:///");

    conversionSourceProvider = new DeltaKernelConversionSourceProvider();
    conversionSourceProvider.init(hadoopConf);
  }

  @Test
  void getCurrentTableTest() {
    // Table name
    final String tableName = GenericTable.getTableName();
    final Path basePath = tempDir.resolve(tableName);
    // Create table with a single row using Spark
    sparkSession.sql(
        "CREATE TABLE `"
            + tableName
            + "` USING DELTA LOCATION '"
            + basePath
            + "' AS SELECT * FROM VALUES (1, 2, '3')");
    // Create Delta source
    SourceTable tableConfig =
        SourceTable.builder()
            .name(tableName)
            .basePath(basePath.toString())
            .formatName(TableFormat.DELTA)
            .build();
    System.out.println(
        "Table Config: " + tableConfig.getBasePath() + ", " + tableConfig.getDataPath());
    DeltaKernelConversionSource conversionSource =
        conversionSourceProvider.getConversionSourceInstance(tableConfig);
    // Get current table
    InternalTable internalTable = conversionSource.getCurrentTable();
    List<InternalField> fields = Arrays.asList(COL1_INT_FIELD, COL2_INT_FIELD, COL3_STR_FIELD);
    System.out.println("Internal Table: " + internalTable);
    System.out.println("Fields: " + fields);
    System.out.println("Table Format: " + TableFormat.DELTA);
    System.out.println("Data Layout Strategy: " + DataLayoutStrategy.FLAT);
    System.out.println("Base Path: " + basePath);
    System.out.println("Latest getReadSchema : " + internalTable.getReadSchema());
    //    System.out.println("Latest getLatestMetadataPath : " + InternalSchema);
    validateTable(
        internalTable,
        tableName,
        TableFormat.DELTA,
        InternalSchema.builder()
            .name("struct")
            .dataType(InternalType.RECORD)
            .fields(fields)
            .build(),
        DataLayoutStrategy.FLAT,
        "file://" + basePath,
        internalTable.getLatestMetadataPath(),
        Collections.emptyList());
  }
}
