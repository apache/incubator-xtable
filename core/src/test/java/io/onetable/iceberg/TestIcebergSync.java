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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import io.onetable.ITOneTableClient;
import io.onetable.client.PerTableConfigImpl;
import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.OneTableMetadata;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.schema.SchemaCatalog;
import io.onetable.model.schema.SchemaVersion;
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
 * Validates that the metadata for the table is properly created/updated. {@link ITOneTableClient}
 * validates that the table and its data can be properly read.
 */
public class TestIcebergSync {
  private static final Random RANDOM = new Random();
  private static final Instant LAST_COMMIT_TIME = Instant.ofEpochSecond(1000);
  private static final Configuration CONFIGURATION = new Configuration();

  @TempDir public static Path tempDir;

  private final IcebergSchemaExtractor mockSchemaExtractor =
      Mockito.mock(IcebergSchemaExtractor.class);
  private final IcebergPartitionSpecExtractor mockPartitionSpecExtractor =
      Mockito.mock(IcebergPartitionSpecExtractor.class);
  private final IcebergSchemaSync mockSchemaSync = Mockito.mock(IcebergSchemaSync.class);
  private final IcebergPartitionSpecSync mockPartitionSpecSync =
      Mockito.mock(IcebergPartitionSpecSync.class);
  private final IcebergColumnStatsConverter mockColumnStatsConverter =
      Mockito.mock(IcebergColumnStatsConverter.class);
  private IcebergClient icebergClient;

  private final OneSchema oneSchema =
      OneSchema.builder()
          .dataType(OneType.RECORD)
          .name("parent")
          .fields(
              Arrays.asList(
                  OneField.builder()
                      .name("timestamp_field")
                      .schema(
                          OneSchema.builder().name("long").dataType(OneType.TIMESTAMP_NTZ).build())
                      .build(),
                  OneField.builder()
                      .name("date_field")
                      .schema(OneSchema.builder().name("int").dataType(OneType.DATE).build())
                      .build(),
                  OneField.builder()
                      .name("group_id")
                      .schema(OneSchema.builder().name("int").dataType(OneType.INT).build())
                      .build(),
                  OneField.builder()
                      .name("record")
                      .schema(
                          OneSchema.builder()
                              .name("nested")
                              .dataType(OneType.RECORD)
                              .fields(
                                  Collections.singletonList(
                                      OneField.builder()
                                          .name("string_field")
                                          .parentPath("record")
                                          .schema(
                                              OneSchema.builder()
                                                  .name("string")
                                                  .dataType(OneType.STRING)
                                                  .build())
                                          .build()))
                              .build())
                      .build()))
          .build();
  private final Schema icebergSchema =
      new Schema(
          Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withoutZone()),
          Types.NestedField.required(2, "date_field", Types.DateType.get()),
          Types.NestedField.required(3, "group_id", Types.IntegerType.get()),
          Types.NestedField.required(
              4,
              "record",
              Types.StructType.of(
                  Types.NestedField.required(5, "string_field", Types.StringType.get()))));
  private Path basePath;
  private String tableName;

  @BeforeEach
  public void setup() throws IOException {
    tableName = "test-" + UUID.randomUUID();
    basePath = tempDir.resolve(tableName);
    Files.createDirectories(basePath);
    icebergClient = getIcebergClient();
  }

  private IcebergClient getIcebergClient() {
    return new IcebergClient(
        PerTableConfigImpl.builder()
            .tableBasePath(basePath.toString())
            .tableName(tableName)
            .targetMetadataRetentionInHours(1)
            .targetTableFormats(Collections.singletonList(TableFormat.ICEBERG))
            .build(),
        CONFIGURATION,
        mockSchemaExtractor,
        mockSchemaSync,
        mockPartitionSpecExtractor,
        mockPartitionSpecSync,
        IcebergDataFileUpdatesSync.of(
            mockColumnStatsConverter, IcebergPartitionValueConverter.getInstance()),
        IcebergTableManager.of(CONFIGURATION));
  }

  @Test
  public void testCreateSnapshotControlFlow() throws Exception {
    // Test two iterations of the iceberg snapshot flow
    List<OneField> fields2 = new ArrayList<>(oneSchema.getFields());
    fields2.add(
        OneField.builder()
            .name("long_field")
            .schema(OneSchema.builder().name("long").dataType(OneType.LONG).build())
            .build());
    OneSchema schema2 = oneSchema.toBuilder().fields(fields2).build();
    List<Types.NestedField> fields = new ArrayList<>(icebergSchema.columns());
    fields.add(Types.NestedField.of(6, false, "long_field", Types.LongType.get()));
    Schema icebergSchema2 = new Schema(fields);
    OneTable table1 = getOneTable(tableName, basePath, oneSchema, null, LAST_COMMIT_TIME);
    OneTable table2 = getOneTable(tableName, basePath, schema2, null, LAST_COMMIT_TIME);
    Map<SchemaVersion, OneSchema> schemas = new HashMap<>();
    SchemaVersion schemaVersion1 = new SchemaVersion(1, "");
    schemas.put(schemaVersion1, oneSchema);
    SchemaVersion schemaVersion2 = new SchemaVersion(2, "");
    schemas.put(schemaVersion2, schema2);

    OneDataFile dataFile1 = getOneDataFile(schemaVersion1, 1, Collections.emptyList());
    OneDataFile dataFile2 = getOneDataFile(schemaVersion1, 2, Collections.emptyList());
    OneDataFile dataFile3 = getOneDataFile(schemaVersion2, 3, Collections.emptyList());
    OneSnapshot snapshot1 = buildSnapshot(table1, schemas, dataFile1, dataFile2);
    OneSnapshot snapshot2 = buildSnapshot(table2, schemas, dataFile2, dataFile3);
    when(mockSchemaExtractor.toIceberg(oneSchema)).thenReturn(icebergSchema);
    when(mockSchemaExtractor.toIceberg(schema2)).thenReturn(icebergSchema2);
    ArgumentCaptor<Schema> partitionSpecSchemaArgumentCaptor =
        ArgumentCaptor.forClass(Schema.class);
    when(mockPartitionSpecExtractor.toIceberg(
            eq(null), partitionSpecSchemaArgumentCaptor.capture()))
        .thenReturn(PartitionSpec.unpartitioned())
        .thenReturn(PartitionSpec.unpartitioned())
        .thenReturn(PartitionSpec.unpartitioned());
    mockColStatsForFile(dataFile1, 2);
    mockColStatsForFile(dataFile2, 2);
    mockColStatsForFile(dataFile3, 1);

    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(icebergClient), snapshot1);
    validateIcebergTable(tableName, table1, Sets.newHashSet(dataFile1, dataFile2), null);

    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(icebergClient), snapshot2);
    validateIcebergTable(tableName, table2, Sets.newHashSet(dataFile2, dataFile3), null);

    ArgumentCaptor<Transaction> transactionArgumentCaptor =
        ArgumentCaptor.forClass(Transaction.class);
    ArgumentCaptor<Schema> schemaArgumentCaptor = ArgumentCaptor.forClass(Schema.class);
    ArgumentCaptor<PartitionSpec> partitionSpecArgumentCaptor =
        ArgumentCaptor.forClass(PartitionSpec.class);

    verify(mockSchemaSync, times(2))
        .sync(
            schemaArgumentCaptor.capture(),
            schemaArgumentCaptor.capture(),
            transactionArgumentCaptor.capture());
    verify(mockPartitionSpecSync, times(2))
        .sync(
            partitionSpecArgumentCaptor.capture(),
            partitionSpecArgumentCaptor.capture(),
            transactionArgumentCaptor.capture());
    verify(mockColumnStatsConverter, times(4)).toIceberg(any(Schema.class), anyLong(), anyList());

    // check that the correct schema is used in calls to the mocks
    // Since we're using a mockSchemaSync we don't expect the table schema used by the partition
    // sync to actually change
    assertTrue(
        partitionSpecSchemaArgumentCaptor.getAllValues().stream()
            .allMatch(capturedSchema -> capturedSchema.sameSchema(icebergSchema)));
    // schema sync args for first iteration
    assertTrue(
        schemaArgumentCaptor.getAllValues().subList(0, 2).stream()
            .allMatch(capturedSchema -> capturedSchema.sameSchema(icebergSchema)));
    // second snapshot sync will evolve the schema
    assertTrue(schemaArgumentCaptor.getAllValues().get(2).sameSchema(icebergSchema));
    assertTrue(schemaArgumentCaptor.getAllValues().get(3).sameSchema(icebergSchema2));
    // check that the correct partition spec is used in calls to the mocks
    assertTrue(
        partitionSpecArgumentCaptor.getAllValues().stream()
            .allMatch(
                capturedPartitionSpec ->
                    capturedPartitionSpec.equals(PartitionSpec.unpartitioned())));
    // validate that a single transaction object was used when updating the schema and partition
    // specs for a given snapshot sync
    assertSame(
        transactionArgumentCaptor.getAllValues().get(0),
        transactionArgumentCaptor.getAllValues().get(2));
    assertSame(
        transactionArgumentCaptor.getAllValues().get(1),
        transactionArgumentCaptor.getAllValues().get(3));
    // validate that transactions are different between runs
    assertNotSame(
        transactionArgumentCaptor.getAllValues().get(1),
        transactionArgumentCaptor.getAllValues().get(2));
  }

  @Test
  public void testIncompleteWriteRollback() throws Exception {
    List<OneField> fields2 = new ArrayList<>(oneSchema.getFields());
    fields2.add(
        OneField.builder()
            .name("long_field")
            .schema(OneSchema.builder().name("long").dataType(OneType.LONG).build())
            .build());
    OneSchema schema2 = oneSchema.toBuilder().fields(fields2).build();
    List<Types.NestedField> fields = new ArrayList<>(icebergSchema.columns());
    fields.add(Types.NestedField.of(6, false, "long_field", Types.LongType.get()));
    Schema icebergSchema2 = new Schema(fields);
    OneTable table1 = getOneTable(tableName, basePath, oneSchema, null, LAST_COMMIT_TIME);
    OneTable table2 =
        getOneTable(tableName, basePath, schema2, null, LAST_COMMIT_TIME.plusMillis(100000L));
    Map<SchemaVersion, OneSchema> schemas = new HashMap<>();
    SchemaVersion schemaVersion1 = new SchemaVersion(1, "");
    schemas.put(schemaVersion1, oneSchema);
    SchemaVersion schemaVersion2 = new SchemaVersion(2, "");
    schemas.put(schemaVersion2, schema2);

    OneDataFile dataFile1 = getOneDataFile(schemaVersion1, 1, Collections.emptyList());
    OneDataFile dataFile2 = getOneDataFile(schemaVersion1, 2, Collections.emptyList());
    OneDataFile dataFile3 = getOneDataFile(schemaVersion2, 3, Collections.emptyList());
    OneDataFile dataFile4 = getOneDataFile(schemaVersion2, 4, Collections.emptyList());
    OneSnapshot snapshot1 = buildSnapshot(table1, schemas, dataFile1, dataFile2);
    OneSnapshot snapshot2 = buildSnapshot(table2, schemas, dataFile2, dataFile3);
    OneSnapshot snapshot3 = buildSnapshot(table2, schemas, dataFile3, dataFile4);
    when(mockSchemaExtractor.toIceberg(oneSchema)).thenReturn(icebergSchema);
    when(mockSchemaExtractor.toIceberg(schema2)).thenReturn(icebergSchema2);
    ArgumentCaptor<Schema> partitionSpecSchemaArgumentCaptor =
        ArgumentCaptor.forClass(Schema.class);
    when(mockPartitionSpecExtractor.toIceberg(
            eq(null), partitionSpecSchemaArgumentCaptor.capture()))
        .thenReturn(PartitionSpec.unpartitioned())
        .thenReturn(PartitionSpec.unpartitioned())
        .thenReturn(PartitionSpec.unpartitioned());
    mockColStatsForFile(dataFile1, 2);
    mockColStatsForFile(dataFile2, 2);
    mockColStatsForFile(dataFile3, 2);
    mockColStatsForFile(dataFile4, 1);

    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(icebergClient), snapshot1);
    long snapshotIdBeforeCorruption = getTable(basePath).currentSnapshot().snapshotId();
    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(icebergClient), snapshot2);
    String manifestFile =
        new HadoopTables(CONFIGURATION)
            .load(basePath.toString())
            .currentSnapshot()
            .manifestListLocation();
    Files.delete(Paths.get(URI.create(manifestFile)));

    Optional<Instant> actual =
        getIcebergClient().getTableMetadata().map(OneTableMetadata::getLastInstantSynced);
    // assert that the last commit is rolled back and the metadata is removed
    assertFalse(actual.isPresent());
    // get a new iceberg sync to make sure table is re-read from disk and no metadata is cached
    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(icebergClient), snapshot3);
    validateIcebergTable(tableName, table2, Sets.newHashSet(dataFile3, dataFile4), null);
    // Validate Iceberg table state
    Table table = getTable(basePath);
    assertEquals(4, table.history().size());
    assertEquals(snapshotIdBeforeCorruption, table.currentSnapshot().parentId());
  }

  @Test
  public void testTimestampPartitioning() throws Exception {
    // test partition filtering
    OnePartitionField partitionField =
        OnePartitionField.builder()
            .sourceField(
                SchemaFieldFinder.getInstance().findFieldByPath(oneSchema, "timestamp_field"))
            .transformType(PartitionTransformType.DAY)
            .build();

    OneTable table =
        getOneTable(
            tableName,
            basePath,
            oneSchema,
            Collections.singletonList(partitionField),
            LAST_COMMIT_TIME);
    Map<SchemaVersion, OneSchema> schemas = new HashMap<>();
    SchemaVersion schemaVersion = new SchemaVersion(1, "");
    schemas.put(schemaVersion, oneSchema);

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
    OneDataFile dataFile1 = getOneDataFile(schemaVersion, 1, partitionValues1);
    OneDataFile dataFile2 = getOneDataFile(schemaVersion, 2, partitionValues1);
    OneDataFile dataFile3 = getOneDataFile(schemaVersion, 3, partitionValues2);
    OneSnapshot snapshot = buildSnapshot(table, schemas, dataFile1, dataFile2, dataFile3);

    when(mockSchemaExtractor.toIceberg(oneSchema))
        .thenReturn(icebergSchema)
        .thenReturn(icebergSchema);
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(icebergSchema)
            .day(partitionField.getSourceField().getName())
            .build();
    ArgumentCaptor<Schema> schemaArgumentCaptor = ArgumentCaptor.forClass(Schema.class);
    when(mockPartitionSpecExtractor.toIceberg(
            eq(Collections.singletonList(partitionField)), schemaArgumentCaptor.capture()))
        .thenReturn(partitionSpec);
    mockColStatsForFile(dataFile1, 1);
    mockColStatsForFile(dataFile2, 1);
    mockColStatsForFile(dataFile3, 1);
    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(icebergClient), snapshot);

    assertTrue(schemaArgumentCaptor.getValue().sameSchema(icebergSchema));
    validateIcebergTable(
        tableName,
        table,
        Sets.newHashSet(dataFile1, dataFile2),
        Expressions.and(
            Expressions.greaterThanOrEqual(
                partitionField.getSourceField().getName(), "2022-10-01T00:00"),
            Expressions.lessThan(partitionField.getSourceField().getName(), "2022-10-02T00:00")));
  }

  @Test
  public void testDatePartitioning() throws Exception {
    // test partition filtering
    OnePartitionField partitionField =
        OnePartitionField.builder()
            .sourceField(SchemaFieldFinder.getInstance().findFieldByPath(oneSchema, "date_field"))
            .transformType(PartitionTransformType.DAY)
            .build();

    OneTable table =
        getOneTable(
            tableName,
            basePath,
            oneSchema,
            Collections.singletonList(partitionField),
            LAST_COMMIT_TIME);
    Map<SchemaVersion, OneSchema> schemas = new HashMap<>();
    SchemaVersion schemaVersion = new SchemaVersion(1, "");
    schemas.put(schemaVersion, oneSchema);

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
    OneDataFile dataFile1 = getOneDataFile(schemaVersion, 1, partitionValues1);
    OneDataFile dataFile2 = getOneDataFile(schemaVersion, 2, partitionValues1);
    OneDataFile dataFile3 = getOneDataFile(schemaVersion, 3, partitionValues2);
    OneSnapshot snapshot = buildSnapshot(table, schemas, dataFile1, dataFile2, dataFile3);

    when(mockSchemaExtractor.toIceberg(oneSchema)).thenReturn(icebergSchema);
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(icebergSchema)
            .day(partitionField.getSourceField().getName())
            .build();
    ArgumentCaptor<Schema> schemaArgumentCaptor = ArgumentCaptor.forClass(Schema.class);
    when(mockPartitionSpecExtractor.toIceberg(
            eq(Collections.singletonList(partitionField)), schemaArgumentCaptor.capture()))
        .thenReturn(partitionSpec);
    mockColStatsForFile(dataFile1, 1);
    mockColStatsForFile(dataFile2, 1);
    mockColStatsForFile(dataFile3, 1);
    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(icebergClient), snapshot);

    assertTrue(schemaArgumentCaptor.getValue().sameSchema(icebergSchema));
    validateIcebergTable(
        tableName,
        table,
        Sets.newHashSet(dataFile1, dataFile2),
        Expressions.and(
            Expressions.greaterThanOrEqual(partitionField.getSourceField().getName(), "2022-10-01"),
            Expressions.lessThan(partitionField.getSourceField().getName(), "2022-10-02")));
  }

  @Test
  public void testNumericFieldPartitioning() throws Exception {
    // test partition filtering
    OnePartitionField partitionField =
        OnePartitionField.builder()
            .sourceField(SchemaFieldFinder.getInstance().findFieldByPath(oneSchema, "group_id"))
            .transformType(PartitionTransformType.VALUE)
            .build();

    OneTable table =
        getOneTable(
            tableName,
            basePath,
            oneSchema,
            Collections.singletonList(partitionField),
            LAST_COMMIT_TIME);
    Map<SchemaVersion, OneSchema> schemas = new HashMap<>();
    SchemaVersion schemaVersion = new SchemaVersion(1, "");
    schemas.put(schemaVersion, oneSchema);

    List<PartitionValue> partitionValues1 =
        Collections.singletonList(
            PartitionValue.builder().partitionField(partitionField).range(Range.scalar(1)).build());
    List<PartitionValue> partitionValues2 =
        Collections.singletonList(
            PartitionValue.builder().partitionField(partitionField).range(Range.scalar(2)).build());
    OneDataFile dataFile1 = getOneDataFile(schemaVersion, 1, partitionValues1);
    OneDataFile dataFile2 = getOneDataFile(schemaVersion, 2, partitionValues1);
    OneDataFile dataFile3 = getOneDataFile(schemaVersion, 3, partitionValues2);
    OneSnapshot snapshot = buildSnapshot(table, schemas, dataFile1, dataFile2, dataFile3);

    when(mockSchemaExtractor.toIceberg(oneSchema)).thenReturn(icebergSchema);
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(icebergSchema)
            .identity(partitionField.getSourceField().getName())
            .build();
    ArgumentCaptor<Schema> schemaArgumentCaptor = ArgumentCaptor.forClass(Schema.class);
    when(mockPartitionSpecExtractor.toIceberg(
            eq(Collections.singletonList(partitionField)), schemaArgumentCaptor.capture()))
        .thenReturn(partitionSpec);
    mockColStatsForFile(dataFile1, 1);
    mockColStatsForFile(dataFile2, 1);
    mockColStatsForFile(dataFile3, 1);
    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(icebergClient), snapshot);

    assertTrue(schemaArgumentCaptor.getValue().sameSchema(icebergSchema));
    validateIcebergTable(
        tableName,
        table,
        Sets.newHashSet(dataFile1, dataFile2),
        Expressions.and(
            Expressions.greaterThanOrEqual(partitionField.getSourceField().getName(), 1),
            Expressions.lessThan(partitionField.getSourceField().getName(), 2)));
  }

  @Test
  public void testMultipleFieldPartitioning() throws Exception {
    // test partition filtering
    OnePartitionField partitionField1 =
        OnePartitionField.builder()
            .sourceField(SchemaFieldFinder.getInstance().findFieldByPath(oneSchema, "group_id"))
            .transformType(PartitionTransformType.VALUE)
            .build();
    OnePartitionField partitionField2 =
        OnePartitionField.builder()
            .sourceField(
                SchemaFieldFinder.getInstance().findFieldByPath(oneSchema, "timestamp_field"))
            .transformType(PartitionTransformType.DAY)
            .build();

    OneTable table =
        getOneTable(
            tableName,
            basePath,
            oneSchema,
            Arrays.asList(partitionField1, partitionField2),
            LAST_COMMIT_TIME);
    Map<SchemaVersion, OneSchema> schemas = new HashMap<>();
    SchemaVersion schemaVersion = new SchemaVersion(1, "");
    schemas.put(schemaVersion, oneSchema);

    List<PartitionValue> partitionValues1 =
        Arrays.asList(
            PartitionValue.builder().partitionField(partitionField1).range(Range.scalar(1)).build(),
            PartitionValue.builder()
                .partitionField(partitionField2)
                .range(Range.scalar(Instant.parse("2022-10-01T00:00:00.00Z").toEpochMilli()))
                .build());
    List<PartitionValue> partitionValues2 =
        Arrays.asList(
            PartitionValue.builder().partitionField(partitionField1).range(Range.scalar(2)).build(),
            PartitionValue.builder()
                .partitionField(partitionField2)
                .range(Range.scalar(Instant.parse("2022-10-01T00:00:00.00Z").toEpochMilli()))
                .build());
    List<PartitionValue> partitionValues3 =
        Arrays.asList(
            PartitionValue.builder().partitionField(partitionField1).range(Range.scalar(2)).build(),
            PartitionValue.builder()
                .partitionField(partitionField2)
                .range(Range.scalar(Instant.parse("2022-10-03T00:00:00.00Z").toEpochMilli()))
                .build());
    OneDataFile dataFile1 = getOneDataFile(schemaVersion, 1, partitionValues1);
    OneDataFile dataFile2 = getOneDataFile(schemaVersion, 2, partitionValues2);
    OneDataFile dataFile3 = getOneDataFile(schemaVersion, 3, partitionValues3);
    OneSnapshot snapshot = buildSnapshot(table, schemas, dataFile1, dataFile2, dataFile3);

    when(mockSchemaExtractor.toIceberg(oneSchema)).thenReturn(icebergSchema);
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(icebergSchema)
            .identity(partitionField1.getSourceField().getName())
            .day(partitionField2.getSourceField().getName())
            .build();
    ArgumentCaptor<Schema> schemaArgumentCaptor = ArgumentCaptor.forClass(Schema.class);
    when(mockPartitionSpecExtractor.toIceberg(
            eq(Arrays.asList(partitionField1, partitionField2)), schemaArgumentCaptor.capture()))
        .thenReturn(partitionSpec);
    mockColStatsForFile(dataFile1, 1);
    mockColStatsForFile(dataFile2, 1);
    mockColStatsForFile(dataFile3, 1);
    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(icebergClient), snapshot);

    assertTrue(schemaArgumentCaptor.getValue().sameSchema(icebergSchema));
    validateIcebergTable(
        tableName,
        table,
        Sets.newHashSet(dataFile2),
        Expressions.and(
            Expressions.equal(partitionField1.getSourceField().getName(), 2),
            Expressions.and(
                Expressions.greaterThanOrEqual(
                    partitionField2.getSourceField().getName(), "2022-10-01T00:00"),
                Expressions.lessThan(
                    partitionField2.getSourceField().getName(), "2022-10-02T00:00"))));
  }

  @Test
  public void testNestedFieldPartitioning() throws Exception {
    // test partition filtering
    OnePartitionField partitionField =
        OnePartitionField.builder()
            .sourceField(
                SchemaFieldFinder.getInstance().findFieldByPath(oneSchema, "record.string_field"))
            .transformType(PartitionTransformType.VALUE)
            .build();

    OneTable table =
        getOneTable(
            tableName,
            basePath,
            oneSchema,
            Collections.singletonList(partitionField),
            LAST_COMMIT_TIME);
    Map<SchemaVersion, OneSchema> schemas = new HashMap<>();
    SchemaVersion schemaVersion = new SchemaVersion(1, "");
    schemas.put(schemaVersion, oneSchema);

    List<PartitionValue> partitionValues1 =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(partitionField)
                .range(Range.scalar("value1"))
                .build());
    List<PartitionValue> partitionValues2 =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(partitionField)
                .range(Range.scalar("value2"))
                .build());
    OneDataFile dataFile1 = getOneDataFile(schemaVersion, 1, partitionValues1);
    OneDataFile dataFile2 = getOneDataFile(schemaVersion, 2, partitionValues1);
    OneDataFile dataFile3 = getOneDataFile(schemaVersion, 3, partitionValues2);
    OneSnapshot snapshot = buildSnapshot(table, schemas, dataFile1, dataFile2, dataFile3);

    when(mockSchemaExtractor.toIceberg(oneSchema)).thenReturn(icebergSchema);
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(icebergSchema)
            .identity(partitionField.getSourceField().getPath())
            .build();
    ArgumentCaptor<Schema> schemaArgumentCaptor = ArgumentCaptor.forClass(Schema.class);
    when(mockPartitionSpecExtractor.toIceberg(
            eq(Collections.singletonList(partitionField)), schemaArgumentCaptor.capture()))
        .thenReturn(partitionSpec);
    mockColStatsForFile(dataFile1, 1);
    mockColStatsForFile(dataFile2, 1);
    mockColStatsForFile(dataFile3, 1);
    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(icebergClient), snapshot);

    assertTrue(schemaArgumentCaptor.getValue().sameSchema(icebergSchema));
    validateIcebergTable(
        tableName,
        table,
        Sets.newHashSet(dataFile1, dataFile2),
        Expressions.equal(partitionField.getSourceField().getPath(), "value1"));
  }

  private OneSnapshot buildSnapshot(
      OneTable table, Map<SchemaVersion, OneSchema> schemas, OneDataFile... dataFiles) {
    return OneSnapshot.builder()
        .table(table)
        .schemaCatalog(SchemaCatalog.builder().schemas(schemas).build())
        .partitionedDataFiles(OneFileGroup.fromFiles(Arrays.asList(dataFiles)))
        .build();
  }

  private OneDataFile getOneDataFile(
      SchemaVersion schemaVersion, int index, List<PartitionValue> partitionValues) {
    String physicalPath = "file:/physical" + index + ".parquet";
    return OneDataFile.builder()
        .fileFormat(FileFormat.APACHE_PARQUET)
        .fileSizeBytes(RANDOM.nextInt(10000))
        .physicalPath(physicalPath)
        .recordCount(RANDOM.nextInt(10000))
        .schemaVersion(schemaVersion)
        .partitionValues(partitionValues)
        .columnStats(Collections.emptyList())
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
        .basePath(basePath.toString())
        .layoutStrategy(DataLayoutStrategy.FLAT)
        .tableFormat(TableFormat.HUDI)
        .readSchema(schema)
        .partitioningFields(partitionFields)
        .latestCommitTime(lastCommitTime)
        .build();
  }

  private void validateIcebergTable(
      String tableName, OneTable table, Set<OneDataFile> expectedFiles, Expression filterExpression)
      throws IOException {
    Path warehouseLocation = Paths.get(table.getBasePath()).getParent();
    try (HadoopCatalog catalog = new HadoopCatalog(CONFIGURATION, warehouseLocation.toString())) {
      TableIdentifier tableId = TableIdentifier.of(Namespace.empty(), tableName);
      assertTrue(catalog.tableExists(tableId));
      TableScan scan = catalog.loadTable(tableId).newScan();
      if (filterExpression != null) {
        scan = scan.filter(filterExpression);
      }
      try (CloseableIterable<CombinedScanTask> tasks = scan.planTasks()) {
        assertEquals(1, Iterables.size(tasks), "1 combined scan task should be generated");
        for (CombinedScanTask combinedScanTask : tasks) {
          assertEquals(expectedFiles.size(), combinedScanTask.files().size());
          Map<String, OneDataFile> pathToFile =
              expectedFiles.stream()
                  .collect(Collectors.toMap(OneDataFile::getPhysicalPath, Function.identity()));
          for (FileScanTask fileScanTask : combinedScanTask.files()) {
            // check that path and other stats match
            OneDataFile expected = pathToFile.get(fileScanTask.file().path());
            assertNotNull(expected);
            assertEquals(expected.getFileSizeBytes(), fileScanTask.file().fileSizeInBytes());
            assertEquals(expected.getRecordCount(), fileScanTask.file().recordCount());
          }
        }
      }
    }
  }

  @SneakyThrows
  private Table getTable(Path basePath) {
    Path warehouseLocation = basePath.getParent();
    try (HadoopCatalog catalog = new HadoopCatalog(CONFIGURATION, warehouseLocation.toString())) {
      TableIdentifier tableId = TableIdentifier.of(Namespace.empty(), tableName);
      assertTrue(catalog.tableExists(tableId));
      return catalog.loadTable(tableId);
    }
  }

  private void mockColStatsForFile(OneDataFile dataFile, int times) {
    Metrics response = new Metrics(dataFile.getRecordCount(), null, null, null, null);
    Metrics[] responses =
        IntStream.of(times - 1).mapToObj(unused -> response).toArray(Metrics[]::new);
    when(mockColumnStatsConverter.toIceberg(
            any(Schema.class), eq(dataFile.getRecordCount()), eq(Collections.emptyList())))
        .thenReturn(response, responses);
  }
}
