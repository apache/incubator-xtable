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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.spark.sql.delta.GeneratedColumn;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.DeltaScan;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.expressions.And;
import io.delta.standalone.expressions.Column;
import io.delta.standalone.expressions.EqualTo;
import io.delta.standalone.expressions.Expression;
import io.delta.standalone.expressions.Literal;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.StringType;

import org.apache.xtable.conversion.PerTableConfigImpl;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.DataLayoutStrategy;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.schema.SchemaFieldFinder;
import org.apache.xtable.spi.sync.TableFormatSync;

/**
 * Validates that the delta log is properly created/updated. Also validates that our assumptions
 * around partitioning are working as expected.
 */
@Execution(SAME_THREAD)
public class TestDeltaSync {
  private static final Random RANDOM = new Random();
  private static final Instant LAST_COMMIT_TIME = Instant.ofEpochSecond(1000);
  private static SparkSession sparkSession;

  @TempDir public static java.nio.file.Path tempDir;
  private DeltaConversionTarget conversionTarget;
  private Path basePath;
  private String tableName;

  @BeforeAll
  public static void setupOnce() {
    sparkSession = buildSparkSession();
  }

  @AfterAll
  public static void teardown() {
    sparkSession.close();
  }

  @BeforeEach
  public void setup() throws IOException {
    tableName = "test-" + UUID.randomUUID();
    basePath = tempDir.resolve(tableName);
    Files.createDirectories(basePath);
    conversionTarget =
        new DeltaConversionTarget(
            PerTableConfigImpl.builder()
                .tableName(tableName)
                .tableBasePath(basePath.toString())
                .targetMetadataRetentionInHours(1)
                .targetTableFormats(Collections.singletonList(TableFormat.DELTA))
                .build(),
            sparkSession);
  }

  @Test
  public void testCreateSnapshotControlFlow() throws Exception {
    InternalSchema schema1 = getInternalSchema();
    List<InternalField> fields2 = new ArrayList<>(schema1.getFields());
    fields2.add(
        InternalField.builder()
            .name("float_field")
            .schema(
                InternalSchema.builder()
                    .name("float")
                    .dataType(InternalType.FLOAT)
                    .isNullable(true)
                    .build())
            .build());
    InternalSchema schema2 = getInternalSchema().toBuilder().fields(fields2).build();
    InternalTable table1 = getInternalTable(tableName, basePath, schema1, null, LAST_COMMIT_TIME);
    InternalTable table2 = getInternalTable(tableName, basePath, schema2, null, LAST_COMMIT_TIME);

    InternalDataFile dataFile1 = getDataFile(1, Collections.emptyList(), basePath);
    InternalDataFile dataFile2 = getDataFile(2, Collections.emptyList(), basePath);
    InternalDataFile dataFile3 = getDataFile(3, Collections.emptyList(), basePath);

    InternalSnapshot snapshot1 = buildSnapshot(table1, dataFile1, dataFile2);
    InternalSnapshot snapshot2 = buildSnapshot(table2, dataFile2, dataFile3);

    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot1);
    validateDeltaTable(basePath, new HashSet<>(Arrays.asList(dataFile1, dataFile2)), null);

    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot2);
    validateDeltaTable(basePath, new HashSet<>(Arrays.asList(dataFile2, dataFile3)), null);
  }

  @Test
  public void testPrimitiveFieldPartitioning() throws Exception {
    InternalSchema schema = getInternalSchema();
    InternalPartitionField internalPartitionField =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("string_field")
                    .schema(
                        InternalSchema.builder()
                            .name("string")
                            .dataType(InternalType.STRING)
                            .build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    InternalTable table =
        getInternalTable(
            tableName,
            basePath,
            schema,
            Collections.singletonList(internalPartitionField),
            LAST_COMMIT_TIME);

    List<PartitionValue> partitionValues1 =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(internalPartitionField)
                .range(Range.scalar("level"))
                .build());
    List<PartitionValue> partitionValues2 =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(internalPartitionField)
                .range(Range.scalar("warning"))
                .build());
    InternalDataFile dataFile1 = getDataFile(1, partitionValues1, basePath);
    InternalDataFile dataFile2 = getDataFile(2, partitionValues1, basePath);
    InternalDataFile dataFile3 = getDataFile(3, partitionValues2, basePath);

    EqualTo equalToExpr =
        new EqualTo(new Column("string_field", new StringType()), Literal.of("warning"));

    InternalSnapshot snapshot1 = buildSnapshot(table, dataFile1, dataFile2, dataFile3);

    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot1);
    validateDeltaTable(basePath, new HashSet<>(Arrays.asList(dataFile3)), equalToExpr);
  }

  @Test
  public void testMultipleFieldPartitioning() throws Exception {
    InternalSchema schema = getInternalSchema();
    InternalPartitionField internalPartitionField1 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("string_field")
                    .schema(
                        InternalSchema.builder()
                            .name("string")
                            .dataType(InternalType.STRING)
                            .build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    InternalPartitionField internalPartitionField2 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("int_field")
                    .schema(InternalSchema.builder().name("int").dataType(InternalType.INT).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    InternalTable table =
        getInternalTable(
            tableName,
            basePath,
            schema,
            Arrays.asList(internalPartitionField1, internalPartitionField2),
            LAST_COMMIT_TIME);

    List<PartitionValue> partitionValues1 =
        Arrays.asList(
            PartitionValue.builder()
                .partitionField(internalPartitionField1)
                .range(Range.scalar("level"))
                .build(),
            PartitionValue.builder()
                .partitionField(internalPartitionField2)
                .range(Range.scalar(10))
                .build());
    List<PartitionValue> partitionValues2 =
        Arrays.asList(
            PartitionValue.builder()
                .partitionField(internalPartitionField1)
                .range(Range.scalar("level"))
                .build(),
            PartitionValue.builder()
                .partitionField(internalPartitionField2)
                .range(Range.scalar(20))
                .build());
    List<PartitionValue> partitionValues3 =
        Arrays.asList(
            PartitionValue.builder()
                .partitionField(internalPartitionField1)
                .range(Range.scalar("warning"))
                .build(),
            PartitionValue.builder()
                .partitionField(internalPartitionField2)
                .range(Range.scalar(20))
                .build());

    InternalDataFile dataFile1 = getDataFile(1, partitionValues1, basePath);
    InternalDataFile dataFile2 = getDataFile(2, partitionValues2, basePath);
    InternalDataFile dataFile3 = getDataFile(3, partitionValues3, basePath);

    EqualTo equalToExpr1 =
        new EqualTo(new Column("string_field", new StringType()), Literal.of("level"));
    EqualTo equalToExpr2 = new EqualTo(new Column("int_field", new IntegerType()), Literal.of(20));
    And CombinedExpr = new And(equalToExpr1, equalToExpr2);

    InternalSnapshot snapshot1 = buildSnapshot(table, dataFile1, dataFile2, dataFile3);
    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot1);
    validateDeltaTable(basePath, new HashSet<>(Arrays.asList(dataFile2)), CombinedExpr);
  }

  // As of delta version 2.1.1, there is no way to use generated partition filters when listing
  // files for a snapshot. Instead we check our assumptions around how to generate partition filters
  // by manually invoking code within Delta that translates filters from the base column to the
  // generated column.
  @ParameterizedTest
  @MethodSource(value = "timestampPartitionTestingArgs")
  public void testTimestampPartitioning(PartitionTransformType transformType) throws Exception {
    InternalSchema schema = getInternalSchema();
    InternalPartitionField partitionField =
        InternalPartitionField.builder()
            .sourceField(SchemaFieldFinder.getInstance().findFieldByPath(schema, "timestamp_field"))
            .transformType(transformType)
            .build();
    InternalTable table =
        getInternalTable(
            tableName,
            basePath,
            schema,
            Collections.singletonList(partitionField),
            LAST_COMMIT_TIME);

    List<PartitionValue> partitionValues1 =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(partitionField)
                .range(Range.scalar(Instant.parse("2022-10-01T00:00:00.00Z").toEpochMilli()))
                .build());
    List<PartitionValue> partitionValues2 =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(partitionField)
                .range(Range.scalar(Instant.parse("2022-10-03T00:00:00.00Z").toEpochMilli()))
                .build());
    InternalDataFile dataFile1 = getDataFile(1, partitionValues1, basePath);
    InternalDataFile dataFile2 = getDataFile(2, partitionValues1, basePath);
    InternalDataFile dataFile3 = getDataFile(3, partitionValues2, basePath);

    InternalSnapshot snapshot1 = buildSnapshot(table, dataFile1, dataFile2, dataFile3);

    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot1);

    Dataset<Row> dataset = sparkSession.read().format("delta").load(basePath.toString());
    org.apache.spark.sql.catalyst.expressions.Expression expression =
        dataset
            .col("timestamp_field")
            .leq(
                org.apache.spark.sql.catalyst.expressions.Literal.create(
                    Instant.parse("2022-10-02T00:00:00.00Z").toEpochMilli() * 1000,
                    DataTypes.TimestampType))
            .expr();
    org.apache.spark.sql.delta.DeltaLog deltaLog =
        org.apache.spark.sql.delta.DeltaLog.forTable(sparkSession, basePath.toString());
    org.apache.spark.sql.delta.Snapshot snapshot = deltaLog.getSnapshotAtInit().snapshot();
    Seq<org.apache.spark.sql.catalyst.expressions.Expression> expressionSeq =
        scala.collection.JavaConversions.asScalaBuffer(Collections.singletonList(expression));
    Seq<org.apache.spark.sql.catalyst.expressions.Expression> translatedExpression =
        GeneratedColumn.generatePartitionFilters(
            sparkSession, snapshot, expressionSeq, dataset.logicalPlan());
    assertEquals(1, translatedExpression.size());
    assertTrue(
        JavaConverters.seqAsJavaList(translatedExpression)
            .get(0)
            .toString()
            .contains(String.format("xtable_partition_col_%s_timestamp_field", transformType)));
  }

  private static Stream<Arguments> timestampPartitionTestingArgs() {
    return Stream.of(
        Arguments.of(PartitionTransformType.YEAR),
        Arguments.of(PartitionTransformType.MONTH),
        Arguments.of(PartitionTransformType.DAY),
        Arguments.of(PartitionTransformType.HOUR));
  }

  private void validateDeltaTable(
      Path basePath, Set<InternalDataFile> internalDataFiles, Expression filterExpression)
      throws IOException {
    DeltaLog deltaLog = DeltaLog.forTable(new Configuration(), basePath.toString());
    assertTrue(deltaLog.tableExists());
    Snapshot snapshot =
        deltaLog.getSnapshotForVersionAsOf(
            deltaLog.getVersionBeforeOrAtTimestamp(Instant.now().toEpochMilli()));
    DeltaScan deltaScan;
    if (filterExpression == null) {
      deltaScan = snapshot.scan();
    } else {
      deltaScan = snapshot.scan(filterExpression);
    }
    Map<String, InternalDataFile> pathToFile =
        internalDataFiles.stream()
            .collect(Collectors.toMap(InternalDataFile::getPhysicalPath, Function.identity()));
    int count = 0;
    try (CloseableIterator<AddFile> fileItr = deltaScan.getFiles()) {
      for (CloseableIterator<AddFile> it = fileItr; it.hasNext(); ) {
        AddFile addFile = it.next();
        String fullPath =
            new org.apache.hadoop.fs.Path(basePath.resolve(addFile.getPath()).toUri()).toString();
        InternalDataFile expected = pathToFile.get(fullPath);
        assertNotNull(expected);
        assertEquals(addFile.getSize(), expected.getFileSizeBytes());
        count++;
      }
    }
    assertEquals(
        internalDataFiles.size(), count, "Number of files from DeltaScan don't match expectation");
  }

  private InternalSnapshot buildSnapshot(InternalTable table, InternalDataFile... dataFiles) {
    return InternalSnapshot.builder()
        .table(table)
        .partitionedDataFiles(PartitionFileGroup.fromFiles(Arrays.asList(dataFiles)))
        .build();
  }

  private InternalTable getInternalTable(
      String tableName,
      Path basePath,
      InternalSchema schema,
      List<InternalPartitionField> partitionFields,
      Instant lastCommitTime) {
    return InternalTable.builder()
        .name(tableName)
        .basePath(basePath.toUri().toString())
        .layoutStrategy(DataLayoutStrategy.FLAT)
        .tableFormat(TableFormat.HUDI)
        .readSchema(schema)
        .partitioningFields(partitionFields)
        .latestCommitTime(lastCommitTime)
        .build();
  }

  private InternalDataFile getDataFile(
      int index, List<PartitionValue> partitionValues, Path basePath) {
    String physicalPath =
        new org.apache.hadoop.fs.Path(basePath.toUri() + "physical" + index + ".parquet")
            .toString();
    return InternalDataFile.builder()
        .fileFormat(FileFormat.APACHE_PARQUET)
        .fileSizeBytes(RANDOM.nextInt(10000))
        .physicalPath(physicalPath)
        .recordCount(RANDOM.nextInt(10000))
        .partitionValues(partitionValues)
        .columnStats(Collections.emptyList())
        .build();
  }

  private InternalSchema getInternalSchema() {
    Map<InternalSchema.MetadataKey, Object> timestampMetadata = new HashMap<>();
    timestampMetadata.put(
        InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MILLIS);
    return InternalSchema.builder()
        .dataType(InternalType.RECORD)
        .name("top_level_schema")
        .fields(
            Arrays.asList(
                InternalField.builder()
                    .name("long_field")
                    .schema(
                        InternalSchema.builder()
                            .name("long")
                            .dataType(InternalType.LONG)
                            .isNullable(true)
                            .build())
                    .build(),
                InternalField.builder()
                    .name("string_field")
                    .schema(
                        InternalSchema.builder()
                            .name("string")
                            .dataType(InternalType.STRING)
                            .isNullable(true)
                            .build())
                    .build(),
                InternalField.builder()
                    .name("int_field")
                    .schema(
                        InternalSchema.builder()
                            .name("int")
                            .dataType(InternalType.INT)
                            .isNullable(true)
                            .build())
                    .build(),
                InternalField.builder()
                    .name("timestamp_field")
                    .schema(
                        InternalSchema.builder()
                            .name("time")
                            .dataType(InternalType.TIMESTAMP)
                            .isNullable(true)
                            .metadata(timestampMetadata)
                            .build())
                    .build()))
        .isNullable(false)
        .build();
  }

  private static SparkSession buildSparkSession() {
    SparkConf sparkConf =
        new SparkConf()
            .setAppName("testdeltasync")
            .set("spark.serializer", KryoSerializer.class.getName())
            .set("spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled", "true")
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .set("spark.master", "local[2]");
    return SparkSession.builder().config(sparkConf).getOrCreate();
  }
}
