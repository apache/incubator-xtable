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

import scala.Option;
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

import io.onetable.client.PerTableConfig;
import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.stat.PartitionValue;
import io.onetable.model.stat.Range;
import io.onetable.model.storage.DataLayoutStrategy;
import io.onetable.model.storage.FileFormat;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneFileGroup;
import io.onetable.model.storage.TableFormat;
import io.onetable.schema.SchemaFieldFinder;
import io.onetable.spi.sync.TableFormatSync;

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
  private DeltaClient deltaClient;
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
    deltaClient =
        new DeltaClient(
            PerTableConfig.builder()
                .tableName(tableName)
                .tableBasePath(basePath.toString())
                .targetMetadataRetentionInHours(1)
                .targetTableFormats(Collections.singletonList(TableFormat.DELTA))
                .build(),
            sparkSession);
  }

  @Test
  public void testCreateSnapshotControlFlow() throws Exception {
    OneSchema schema1 = getOneSchema();
    List<OneField> fields2 = new ArrayList<>(schema1.getFields());
    fields2.add(
        OneField.builder()
            .name("float_field")
            .schema(
                OneSchema.builder().name("float").dataType(OneType.FLOAT).isNullable(true).build())
            .build());
    OneSchema schema2 = getOneSchema().toBuilder().fields(fields2).build();
    OneTable table1 = getOneTable(tableName, basePath, schema1, null, LAST_COMMIT_TIME);
    OneTable table2 = getOneTable(tableName, basePath, schema2, null, LAST_COMMIT_TIME);

    OneDataFile dataFile1 = getOneDataFile(1, Collections.emptyList(), basePath);
    OneDataFile dataFile2 = getOneDataFile(2, Collections.emptyList(), basePath);
    OneDataFile dataFile3 = getOneDataFile(3, Collections.emptyList(), basePath);

    OneSnapshot snapshot1 = buildSnapshot(table1, dataFile1, dataFile2);
    OneSnapshot snapshot2 = buildSnapshot(table2, dataFile2, dataFile3);

    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(deltaClient), snapshot1);
    validateDeltaTable(basePath, new HashSet<>(Arrays.asList(dataFile1, dataFile2)), null);

    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(deltaClient), snapshot2);
    validateDeltaTable(basePath, new HashSet<>(Arrays.asList(dataFile2, dataFile3)), null);
  }

  @Test
  public void testPrimitiveFieldPartitioning() throws Exception {
    OneSchema schema = getOneSchema();
    OnePartitionField onePartitionField =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("string_field")
                    .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    OneTable table =
        getOneTable(
            tableName,
            basePath,
            schema,
            Collections.singletonList(onePartitionField),
            LAST_COMMIT_TIME);

    List<PartitionValue> partitionValues1 =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(onePartitionField)
                .range(Range.scalar("level"))
                .build());
    List<PartitionValue> partitionValues2 =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(onePartitionField)
                .range(Range.scalar("warning"))
                .build());
    OneDataFile dataFile1 = getOneDataFile(1, partitionValues1, basePath);
    OneDataFile dataFile2 = getOneDataFile(2, partitionValues1, basePath);
    OneDataFile dataFile3 = getOneDataFile(3, partitionValues2, basePath);

    EqualTo equalToExpr =
        new EqualTo(new Column("string_field", new StringType()), Literal.of("warning"));

    OneSnapshot snapshot1 = buildSnapshot(table, dataFile1, dataFile2, dataFile3);

    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(deltaClient), snapshot1);
    validateDeltaTable(basePath, new HashSet<>(Arrays.asList(dataFile3)), equalToExpr);
  }

  @Test
  public void testMultipleFieldPartitioning() throws Exception {
    OneSchema schema = getOneSchema();
    OnePartitionField onePartitionField1 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("string_field")
                    .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    OnePartitionField onePartitionField2 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("int_field")
                    .schema(OneSchema.builder().name("int").dataType(OneType.INT).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    OneTable table =
        getOneTable(
            tableName,
            basePath,
            schema,
            Arrays.asList(onePartitionField1, onePartitionField2),
            LAST_COMMIT_TIME);

    List<PartitionValue> partitionValues1 =
        Arrays.asList(
            PartitionValue.builder()
                .partitionField(onePartitionField1)
                .range(Range.scalar("level"))
                .build(),
            PartitionValue.builder()
                .partitionField(onePartitionField2)
                .range(Range.scalar(10))
                .build());
    List<PartitionValue> partitionValues2 =
        Arrays.asList(
            PartitionValue.builder()
                .partitionField(onePartitionField1)
                .range(Range.scalar("level"))
                .build(),
            PartitionValue.builder()
                .partitionField(onePartitionField2)
                .range(Range.scalar(20))
                .build());
    List<PartitionValue> partitionValues3 =
        Arrays.asList(
            PartitionValue.builder()
                .partitionField(onePartitionField1)
                .range(Range.scalar("warning"))
                .build(),
            PartitionValue.builder()
                .partitionField(onePartitionField2)
                .range(Range.scalar(20))
                .build());

    OneDataFile dataFile1 = getOneDataFile(1, partitionValues1, basePath);
    OneDataFile dataFile2 = getOneDataFile(2, partitionValues2, basePath);
    OneDataFile dataFile3 = getOneDataFile(3, partitionValues3, basePath);

    EqualTo equalToExpr1 =
        new EqualTo(new Column("string_field", new StringType()), Literal.of("level"));
    EqualTo equalToExpr2 = new EqualTo(new Column("int_field", new IntegerType()), Literal.of(20));
    And CombinedExpr = new And(equalToExpr1, equalToExpr2);

    OneSnapshot snapshot1 = buildSnapshot(table, dataFile1, dataFile2, dataFile3);
    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(deltaClient), snapshot1);
    validateDeltaTable(basePath, new HashSet<>(Arrays.asList(dataFile2)), CombinedExpr);
  }

  // As of delta version 2.1.1, there is no way to use generated partition filters when listing
  // files for a snapshot. Instead we check our assumptions around how to generate partition filters
  // by manually invoking code within Delta that translates filters from the base column to the
  // generated column.
  @ParameterizedTest
  @MethodSource(value = "timestampPartitionTestingArgs")
  public void testTimestampPartitioning(PartitionTransformType transformType) throws Exception {
    OneSchema schema = getOneSchema();
    OnePartitionField partitionField =
        OnePartitionField.builder()
            .sourceField(SchemaFieldFinder.getInstance().findFieldByPath(schema, "timestamp_field"))
            .transformType(transformType)
            .build();
    OneTable table =
        getOneTable(
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
    OneDataFile dataFile1 = getOneDataFile(1, partitionValues1, basePath);
    OneDataFile dataFile2 = getOneDataFile(2, partitionValues1, basePath);
    OneDataFile dataFile3 = getOneDataFile(3, partitionValues2, basePath);

    OneSnapshot snapshot1 = buildSnapshot(table, dataFile1, dataFile2, dataFile3);

    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(deltaClient), snapshot1);

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
    org.apache.spark.sql.delta.Snapshot snapshot =
        deltaLog.getSnapshotAtInit(Option.empty()).snapshot();
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
            .contains(String.format("onetable_partition_col_%s_timestamp_field", transformType)));
  }

  private static Stream<Arguments> timestampPartitionTestingArgs() {
    return Stream.of(
        Arguments.of(PartitionTransformType.YEAR),
        Arguments.of(PartitionTransformType.MONTH),
        Arguments.of(PartitionTransformType.DAY),
        Arguments.of(PartitionTransformType.HOUR));
  }

  private void validateDeltaTable(
      Path basePath, Set<OneDataFile> oneDataFiles, Expression filterExpression)
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
    Map<String, OneDataFile> pathToFile =
        oneDataFiles.stream()
            .collect(Collectors.toMap(OneDataFile::getPhysicalPath, Function.identity()));
    int count = 0;
    try (CloseableIterator<AddFile> fileItr = deltaScan.getFiles()) {
      for (CloseableIterator<AddFile> it = fileItr; it.hasNext(); ) {
        AddFile addFile = it.next();
        String fullPath =
            new org.apache.hadoop.fs.Path(basePath.resolve(addFile.getPath()).toUri()).toString();
        OneDataFile expected = pathToFile.get(fullPath);
        assertNotNull(expected);
        assertEquals(addFile.getSize(), expected.getFileSizeBytes());
        count++;
      }
    }
    assertEquals(
        oneDataFiles.size(), count, "Number of files from DeltaScan don't match expectation");
  }

  private OneSnapshot buildSnapshot(OneTable table, OneDataFile... dataFiles) {
    return OneSnapshot.builder()
        .table(table)
        .partitionedDataFiles(OneFileGroup.fromFiles(Arrays.asList(dataFiles)))
        .build();
  }

  private OneTable getOneTable(
      String tableName,
      Path basePath,
      OneSchema schema,
      List<OnePartitionField> partitionFields,
      Instant lastCommitTime) {
    return OneTable.builder()
        .name(tableName)
        .basePath(basePath.toUri().toString())
        .layoutStrategy(DataLayoutStrategy.FLAT)
        .tableFormat(TableFormat.HUDI)
        .readSchema(schema)
        .partitioningFields(partitionFields)
        .latestCommitTime(lastCommitTime)
        .build();
  }

  private OneDataFile getOneDataFile(
      int index, List<PartitionValue> partitionValues, Path basePath) {
    String physicalPath =
        new org.apache.hadoop.fs.Path(basePath.toUri() + "physical" + index + ".parquet")
            .toString();
    return OneDataFile.builder()
        .fileFormat(FileFormat.APACHE_PARQUET)
        .fileSizeBytes(RANDOM.nextInt(10000))
        .physicalPath(physicalPath)
        .recordCount(RANDOM.nextInt(10000))
        .partitionValues(partitionValues)
        .columnStats(Collections.emptyList())
        .build();
  }

  private OneSchema getOneSchema() {
    Map<OneSchema.MetadataKey, Object> timestampMetadata = new HashMap<>();
    timestampMetadata.put(
        OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MILLIS);
    return OneSchema.builder()
        .dataType(OneType.RECORD)
        .name("top_level_schema")
        .fields(
            Arrays.asList(
                OneField.builder()
                    .name("long_field")
                    .schema(
                        OneSchema.builder()
                            .name("long")
                            .dataType(OneType.LONG)
                            .isNullable(true)
                            .build())
                    .build(),
                OneField.builder()
                    .name("string_field")
                    .schema(
                        OneSchema.builder()
                            .name("string")
                            .dataType(OneType.STRING)
                            .isNullable(true)
                            .build())
                    .build(),
                OneField.builder()
                    .name("int_field")
                    .schema(
                        OneSchema.builder()
                            .name("int")
                            .dataType(OneType.INT)
                            .isNullable(true)
                            .build())
                    .build(),
                OneField.builder()
                    .name("timestamp_field")
                    .schema(
                        OneSchema.builder()
                            .name("time")
                            .dataType(OneType.TIMESTAMP)
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
            .set("spark.master", "local[2]");
    return SparkSession.builder().config(sparkConf).getOrCreate();
  }
}
