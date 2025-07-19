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

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import io.delta.kernel.*;

import org.apache.xtable.GenericTable;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.kernel.DeltaKernelConversionSource;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.Range;
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
                  .name("integer")
                  .dataType(InternalType.INT)
                  .isNullable(true)
                  .build())
          .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
          .build();
  private static final ColumnStat COL2_COLUMN_STAT =
      ColumnStat.builder()
          .field(COL2_INT_FIELD)
          .range(Range.vector(2, 2))
          .numNulls(0)
          .numValues(1)
          .totalSize(0)
          .build();
  private static final ColumnStat COL1_COLUMN_STAT =
      ColumnStat.builder()
          .field(COL1_INT_FIELD)
          .range(Range.vector(1, 1))
          .numNulls(0)
          .numValues(1)
          .totalSize(0)
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

  @AfterAll
  public static void teardown() {
    if (sparkSession != null) {
      sparkSession.close();
    }
  }

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
            + "' AS SELECT * FROM VALUES (1, 2, 3)");
    // Create Delta source
    SourceTable tableConfig =
        SourceTable.builder()
            .name(tableName)
            .basePath(basePath.toString())
            .formatName(TableFormat.DELTA)
            .build();
    //    System.out.println(
    //        "Table Config: " + tableConfig.getBasePath() + ", " + tableConfig.getDataPath());
    DeltaKernelConversionSource conversionSource =
        conversionSourceProvider.getConversionSourceInstance(tableConfig);
    // Get current table
    InternalTable internalTable = conversionSource.getCurrentTable();
    List<InternalField> fields = Arrays.asList(COL1_INT_FIELD, COL2_INT_FIELD, COL3_STR_FIELD);
    //    System.out.println("Internal Table: " + internalTable);
    //    System.out.println("Fields: " + fields);
    //    System.out.println("Table Format: " + TableFormat.DELTA);
    //    System.out.println("Data Layout Strategy: " + DataLayoutStrategy.FLAT);
    //    System.out.println("Base Path: " + basePath);
    //    System.out.println("Latest getReadSchema : " + internalTable.getReadSchema());
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

  @Test
  void getCurrentSnapshotNonPartitionedTest() throws URISyntaxException {
    // Table name
    final String tableName = GenericTable.getTableName();
    final Path basePath = tempDir.resolve(tableName);

    System.out.println("Table Name: " + tableName);
    System.out.println("Base Path: " + basePath);
    // Create table with a single row using Spark
    sparkSession.sql(
        "CREATE TABLE `"
            + tableName
            + "` USING DELTA LOCATION '"
            + basePath
            + "' AS SELECT * FROM VALUES (1, 2)");
    // Create Delta source
    SourceTable tableConfig =
        SourceTable.builder()
            .name(tableName)
            .basePath(basePath.toString())
            .formatName(TableFormat.DELTA)
            .build();
    DeltaKernelConversionSource conversionSource =
        conversionSourceProvider.getConversionSourceInstance(tableConfig);
    // Get current snapshot
    InternalSnapshot snapshot = conversionSource.getCurrentSnapshot();

    //    snapshot.getPartitionedDataFiles().get(0)
    // Validate table
    List<InternalField> fields = Arrays.asList(COL1_INT_FIELD, COL2_INT_FIELD);
    validateTable(
        snapshot.getTable(),
        tableName,
        TableFormat.DELTA,
        InternalSchema.builder()
            .name("struct")
            .dataType(InternalType.RECORD)
            .fields(fields)
            .build(),
        DataLayoutStrategy.FLAT,
        "file://" + basePath,
        snapshot.getTable().getLatestMetadataPath(),
        Collections.emptyList());
    // Validate data files
    List<ColumnStat> columnStats = Arrays.asList(COL1_COLUMN_STAT, COL2_COLUMN_STAT);
    Assertions.assertEquals(1, snapshot.getPartitionedDataFiles().size());

//    validatePartitionDataFiles(
//        PartitionFileGroup.builder()
//            .files(
//                Collections.singletonList(
//                    InternalDataFile.builder()
//                        .physicalPath("file:/fake/path")
//                        .fileFormat(FileFormat.APACHE_PARQUET)
//                        .partitionValues(Collections.emptyList())
//                        .fileSizeBytes(716)
//                        .recordCount(1)
//                        .columnStats(columnStats)
//                        .build()))
//            .partitionValues(Collections.emptyList())
//            .build(),
//        snapshot.getPartitionedDataFiles().get(0));
    //    System.out.println(snapshot.getPartitionedDataFiles().get(0).getDataFiles());
    //    Configuration hadoopConf = new Configuration();
    //    Engine myEngine = DefaultEngine.create(hadoopConf);
    //    Table myTable = Table.forPath(myEngine, basePath.toString());
    //    Snapshot mySnapshot = myTable.getLatestSnapshot(myEngine);
    //    Scan myScan = mySnapshot.getScanBuilder().build();
    //
    //
    //    // Common information about scanning for all data files to read.
    //    Row scanState = myScan.getScanState(myEngine);
    //
    //    // Information about the list of scan files to read
    //    CloseableIterator<FilteredColumnarBatch> fileIter = myScan.getScanFiles(myEngine);
    //    int readRecordCount = 0;
    //    try {
    //      StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(myEngine,
    // scanState);
    //      while (fileIter.hasNext()) {
    //        FilteredColumnarBatch scanFilesBatch = fileIter.next();
    //        try (CloseableIterator<Row> scanFileRows = scanFilesBatch.getRows()) {
    //          while (scanFileRows.hasNext()) {
    //            Row scanFileRow = scanFileRows.next();
    //            FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
    //            CloseableIterator<ColumnarBatch> physicalDataIter =
    //                    myEngine
    //                            .getParquetHandler()
    //                            .readParquetFiles(
    //                                    singletonCloseableIterator(fileStatus),
    //                                    physicalReadSchema,
    //                                    Optional.empty());
    //            try (CloseableIterator<FilteredColumnarBatch> transformedData =
    //                         Scan.transformPhysicalData(myEngine, scanState, scanFileRow,
    // physicalDataIter)) {
    //              while (transformedData.hasNext()) {
    //                FilteredColumnarBatch logicalData = transformedData.next();
    //                ColumnarBatch dataBatch = logicalData.getData();
    //
    //                // access the data for the column at ordinal 0
    //                ColumnVector column0 = dataBatch.getColumnVector(0);
    //                ColumnVector column1 = dataBatch.getColumnVector(1);
    ////
    ////                for (int rowIndex = 0; rowIndex < column0.getSize(); rowIndex++) {
    ////                  System.out.println(column0.getInt(rowIndex));
    ////                }
    //                for (int rowIndex = 0; rowIndex < column1.getSize(); rowIndex++) {
    //                  System.out.println(column1.getInt(rowIndex));
    //                }
    //              }
    //            }
    //          }
    //        }
    //      }
    //    } catch (IOException e) {
    //      e.printStackTrace();
    //      System.out.println("IOException occurred: " + e.getMessage());
    //    }

  }

  private void validatePartitionDataFiles(
      PartitionFileGroup expectedPartitionFiles, PartitionFileGroup actualPartitionFiles)
      throws URISyntaxException {
    assertEquals(
        expectedPartitionFiles.getPartitionValues(), actualPartitionFiles.getPartitionValues());
    validateDataFiles(expectedPartitionFiles.getDataFiles(), actualPartitionFiles.getDataFiles());
  }

  private void validateDataFiles(
      List<InternalDataFile> expectedFiles, List<InternalDataFile> actualFiles)
      throws URISyntaxException {
    Assertions.assertEquals(expectedFiles.size(), actualFiles.size());
    for (int i = 0; i < expectedFiles.size(); i++) {
      InternalDataFile expected = expectedFiles.get(i);
      InternalDataFile actual = actualFiles.get(i);
      validatePropertiesDataFile(expected, actual);
    }
  }

  private void validatePropertiesDataFile(InternalDataFile expected, InternalDataFile actual)
      throws URISyntaxException {
    Assertions.assertTrue(
        Paths.get(new URI(actual.getPhysicalPath()).getPath()).isAbsolute(),
        () -> "path == " + actual.getPhysicalPath() + " is not absolute");
    Assertions.assertEquals(expected.getFileFormat(), actual.getFileFormat());
    Assertions.assertEquals(expected.getPartitionValues(), actual.getPartitionValues());
    Assertions.assertEquals(expected.getFileSizeBytes(), actual.getFileSizeBytes());
    System.out.println("Expected File Size: " + expected);
    System.out.println("Actual File Size: " + actual);
    //    Assertions.assertEquals(expected.getRecordCount(), actual.getRecordCount());
    Instant now = Instant.now();
    long minRange = now.minus(1, ChronoUnit.HOURS).toEpochMilli();
    long maxRange = now.toEpochMilli();
    Assertions.assertTrue(
        actual.getLastModified() > minRange && actual.getLastModified() <= maxRange,
        () ->
            "last modified == "
                + actual.getLastModified()
                + " is expected between "
                + minRange
                + " and "
                + maxRange);
    Assertions.assertEquals(expected.getColumnStats(), actual.getColumnStats());
  }
}
