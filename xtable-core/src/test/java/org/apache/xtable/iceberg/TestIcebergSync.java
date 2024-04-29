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
 
package org.apache.xtable.iceberg;

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

import org.apache.xtable.ITConversionController;
import org.apache.xtable.conversion.PerTableConfigImpl;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.metadata.TableSyncMetadata;
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
 * Validates that the metadata for the table is properly created/updated. {@link
 * ITConversionController} validates that the table and its data can be properly read.
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
  private IcebergConversionTarget conversionTarget;

  private final InternalSchema internalSchema =
      InternalSchema.builder()
          .dataType(InternalType.RECORD)
          .name("parent")
          .fields(
              Arrays.asList(
                  InternalField.builder()
                      .name("timestamp_field")
                      .schema(
                          InternalSchema.builder()
                              .name("long")
                              .dataType(InternalType.TIMESTAMP_NTZ)
                              .build())
                      .build(),
                  InternalField.builder()
                      .name("date_field")
                      .schema(
                          InternalSchema.builder().name("int").dataType(InternalType.DATE).build())
                      .build(),
                  InternalField.builder()
                      .name("group_id")
                      .schema(
                          InternalSchema.builder().name("int").dataType(InternalType.INT).build())
                      .build(),
                  InternalField.builder()
                      .name("record")
                      .schema(
                          InternalSchema.builder()
                              .name("nested")
                              .dataType(InternalType.RECORD)
                              .fields(
                                  Collections.singletonList(
                                      InternalField.builder()
                                          .name("string_field")
                                          .parentPath("record")
                                          .schema(
                                              InternalSchema.builder()
                                                  .name("string")
                                                  .dataType(InternalType.STRING)
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
    conversionTarget = getConversionTarget();
  }

  private IcebergConversionTarget getConversionTarget() {
    return new IcebergConversionTarget(
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
    List<InternalField> fields2 = new ArrayList<>(internalSchema.getFields());
    fields2.add(
        InternalField.builder()
            .name("long_field")
            .schema(InternalSchema.builder().name("long").dataType(InternalType.LONG).build())
            .build());
    InternalSchema schema2 = internalSchema.toBuilder().fields(fields2).build();
    List<Types.NestedField> fields = new ArrayList<>(icebergSchema.columns());
    fields.add(Types.NestedField.of(6, false, "long_field", Types.LongType.get()));
    Schema icebergSchema2 = new Schema(fields);
    InternalTable table1 =
        getInternalTable(tableName, basePath, internalSchema, null, LAST_COMMIT_TIME);
    InternalTable table2 = getInternalTable(tableName, basePath, schema2, null, LAST_COMMIT_TIME);

    InternalDataFile dataFile1 = getDataFile(1, Collections.emptyList());
    InternalDataFile dataFile2 = getDataFile(2, Collections.emptyList());
    InternalDataFile dataFile3 = getDataFile(3, Collections.emptyList());
    InternalSnapshot snapshot1 = buildSnapshot(table1, dataFile1, dataFile2);
    InternalSnapshot snapshot2 = buildSnapshot(table2, dataFile2, dataFile3);
    when(mockSchemaExtractor.toIceberg(internalSchema)).thenReturn(icebergSchema);
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

    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot1);
    validateIcebergTable(tableName, table1, Sets.newHashSet(dataFile1, dataFile2), null);

    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot2);
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
    verify(mockColumnStatsConverter, times(3)).toIceberg(any(Schema.class), anyLong(), anyList());

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
    List<InternalField> fields2 = new ArrayList<>(internalSchema.getFields());
    fields2.add(
        InternalField.builder()
            .name("long_field")
            .schema(InternalSchema.builder().name("long").dataType(InternalType.LONG).build())
            .build());
    InternalSchema schema2 = internalSchema.toBuilder().fields(fields2).build();
    List<Types.NestedField> fields = new ArrayList<>(icebergSchema.columns());
    fields.add(Types.NestedField.of(6, false, "long_field", Types.LongType.get()));
    Schema icebergSchema2 = new Schema(fields);
    InternalTable table1 =
        getInternalTable(tableName, basePath, internalSchema, null, LAST_COMMIT_TIME);
    InternalTable table2 =
        getInternalTable(tableName, basePath, schema2, null, LAST_COMMIT_TIME.plusMillis(100000L));

    InternalDataFile dataFile1 = getDataFile(1, Collections.emptyList());
    InternalDataFile dataFile2 = getDataFile(2, Collections.emptyList());
    InternalDataFile dataFile3 = getDataFile(3, Collections.emptyList());
    InternalDataFile dataFile4 = getDataFile(4, Collections.emptyList());
    InternalSnapshot snapshot1 = buildSnapshot(table1, dataFile1, dataFile2);
    InternalSnapshot snapshot2 = buildSnapshot(table2, dataFile2, dataFile3);
    InternalSnapshot snapshot3 = buildSnapshot(table2, dataFile3, dataFile4);
    when(mockSchemaExtractor.toIceberg(internalSchema)).thenReturn(icebergSchema);
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

    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot1);
    long snapshotIdBeforeCorruption = getTable(basePath).currentSnapshot().snapshotId();
    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot2);
    String manifestFile =
        new HadoopTables(CONFIGURATION)
            .load(basePath.toString())
            .currentSnapshot()
            .manifestListLocation();
    Files.delete(Paths.get(URI.create(manifestFile)));

    Optional<Instant> actual =
        getConversionTarget().getTableMetadata().map(TableSyncMetadata::getLastInstantSynced);
    // assert that the last commit is rolled back and the metadata is removed
    assertFalse(actual.isPresent());
    // get a new iceberg sync to make sure table is re-read from disk and no metadata is cached
    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot3);
    validateIcebergTable(tableName, table2, Sets.newHashSet(dataFile3, dataFile4), null);
    // Validate Iceberg table state
    Table table = getTable(basePath);
    assertEquals(4, table.history().size());
    assertEquals(snapshotIdBeforeCorruption, table.currentSnapshot().parentId());
  }

  @Test
  public void testTimestampPartitioning() throws Exception {
    // test partition filtering
    InternalPartitionField partitionField =
        InternalPartitionField.builder()
            .sourceField(
                SchemaFieldFinder.getInstance().findFieldByPath(internalSchema, "timestamp_field"))
            .transformType(PartitionTransformType.DAY)
            .build();

    InternalTable table =
        getInternalTable(
            tableName,
            basePath,
            internalSchema,
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
    InternalDataFile dataFile1 = getDataFile(1, partitionValues1);
    InternalDataFile dataFile2 = getDataFile(2, partitionValues1);
    InternalDataFile dataFile3 = getDataFile(3, partitionValues2);
    InternalSnapshot snapshot = buildSnapshot(table, dataFile1, dataFile2, dataFile3);

    when(mockSchemaExtractor.toIceberg(internalSchema))
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
    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot);

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
    InternalPartitionField partitionField =
        InternalPartitionField.builder()
            .sourceField(
                SchemaFieldFinder.getInstance().findFieldByPath(internalSchema, "date_field"))
            .transformType(PartitionTransformType.DAY)
            .build();

    InternalTable table =
        getInternalTable(
            tableName,
            basePath,
            internalSchema,
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
    InternalDataFile dataFile1 = getDataFile(1, partitionValues1);
    InternalDataFile dataFile2 = getDataFile(2, partitionValues1);
    InternalDataFile dataFile3 = getDataFile(3, partitionValues2);
    InternalSnapshot snapshot = buildSnapshot(table, dataFile1, dataFile2, dataFile3);

    when(mockSchemaExtractor.toIceberg(internalSchema)).thenReturn(icebergSchema);
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
    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot);

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
    InternalPartitionField partitionField =
        InternalPartitionField.builder()
            .sourceField(
                SchemaFieldFinder.getInstance().findFieldByPath(internalSchema, "group_id"))
            .transformType(PartitionTransformType.VALUE)
            .build();

    InternalTable table =
        getInternalTable(
            tableName,
            basePath,
            internalSchema,
            Collections.singletonList(partitionField),
            LAST_COMMIT_TIME);

    List<PartitionValue> partitionValues1 =
        Collections.singletonList(
            PartitionValue.builder().partitionField(partitionField).range(Range.scalar(1)).build());
    List<PartitionValue> partitionValues2 =
        Collections.singletonList(
            PartitionValue.builder().partitionField(partitionField).range(Range.scalar(2)).build());
    InternalDataFile dataFile1 = getDataFile(1, partitionValues1);
    InternalDataFile dataFile2 = getDataFile(2, partitionValues1);
    InternalDataFile dataFile3 = getDataFile(3, partitionValues2);
    InternalSnapshot snapshot = buildSnapshot(table, dataFile1, dataFile2, dataFile3);

    when(mockSchemaExtractor.toIceberg(internalSchema)).thenReturn(icebergSchema);
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
    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot);

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
    InternalPartitionField partitionField1 =
        InternalPartitionField.builder()
            .sourceField(
                SchemaFieldFinder.getInstance().findFieldByPath(internalSchema, "group_id"))
            .transformType(PartitionTransformType.VALUE)
            .build();
    InternalPartitionField partitionField2 =
        InternalPartitionField.builder()
            .sourceField(
                SchemaFieldFinder.getInstance().findFieldByPath(internalSchema, "timestamp_field"))
            .transformType(PartitionTransformType.DAY)
            .build();

    InternalTable table =
        getInternalTable(
            tableName,
            basePath,
            internalSchema,
            Arrays.asList(partitionField1, partitionField2),
            LAST_COMMIT_TIME);

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
    InternalDataFile dataFile1 = getDataFile(1, partitionValues1);
    InternalDataFile dataFile2 = getDataFile(2, partitionValues2);
    InternalDataFile dataFile3 = getDataFile(3, partitionValues3);
    InternalSnapshot snapshot = buildSnapshot(table, dataFile1, dataFile2, dataFile3);

    when(mockSchemaExtractor.toIceberg(internalSchema)).thenReturn(icebergSchema);
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
    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot);

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
    InternalPartitionField partitionField =
        InternalPartitionField.builder()
            .sourceField(
                SchemaFieldFinder.getInstance()
                    .findFieldByPath(internalSchema, "record.string_field"))
            .transformType(PartitionTransformType.VALUE)
            .build();

    InternalTable table =
        getInternalTable(
            tableName,
            basePath,
            internalSchema,
            Collections.singletonList(partitionField),
            LAST_COMMIT_TIME);

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
    InternalDataFile dataFile1 = getDataFile(1, partitionValues1);
    InternalDataFile dataFile2 = getDataFile(2, partitionValues1);
    InternalDataFile dataFile3 = getDataFile(3, partitionValues2);
    InternalSnapshot snapshot = buildSnapshot(table, dataFile1, dataFile2, dataFile3);

    when(mockSchemaExtractor.toIceberg(internalSchema)).thenReturn(icebergSchema);
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
    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot);

    assertTrue(schemaArgumentCaptor.getValue().sameSchema(icebergSchema));
    validateIcebergTable(
        tableName,
        table,
        Sets.newHashSet(dataFile1, dataFile2),
        Expressions.equal(partitionField.getSourceField().getPath(), "value1"));
  }

  private InternalSnapshot buildSnapshot(InternalTable table, InternalDataFile... dataFiles) {
    return InternalSnapshot.builder()
        .table(table)
        .partitionedDataFiles(PartitionFileGroup.fromFiles(Arrays.asList(dataFiles)))
        .build();
  }

  private InternalDataFile getDataFile(int index, List<PartitionValue> partitionValues) {
    String physicalPath = "file:/physical" + index + ".parquet";
    return InternalDataFile.builder()
        .fileFormat(FileFormat.APACHE_PARQUET)
        .fileSizeBytes(RANDOM.nextInt(10000))
        .physicalPath(physicalPath)
        .recordCount(RANDOM.nextInt(10000))
        .partitionValues(partitionValues)
        .columnStats(Collections.emptyList())
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
        .basePath(basePath.toString())
        .layoutStrategy(DataLayoutStrategy.FLAT)
        .tableFormat(TableFormat.HUDI)
        .readSchema(schema)
        .partitioningFields(partitionFields)
        .latestCommitTime(lastCommitTime)
        .build();
  }

  private void validateIcebergTable(
      String tableName,
      InternalTable table,
      Set<InternalDataFile> expectedFiles,
      Expression filterExpression)
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
          Map<String, InternalDataFile> pathToFile =
              expectedFiles.stream()
                  .collect(
                      Collectors.toMap(InternalDataFile::getPhysicalPath, Function.identity()));
          for (FileScanTask fileScanTask : combinedScanTask.files()) {
            // check that path and other stats match
            InternalDataFile expected = pathToFile.get(fileScanTask.file().path());
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

  private void mockColStatsForFile(InternalDataFile dataFile, int times) {
    Metrics response = new Metrics(dataFile.getRecordCount(), null, null, null, null);
    Metrics[] responses =
        IntStream.of(times - 1).mapToObj(unused -> response).toArray(Metrics[]::new);
    when(mockColumnStatsConverter.toIceberg(
            any(Schema.class), eq(dataFile.getRecordCount()), eq(Collections.emptyList())))
        .thenReturn(response, responses);
  }
}
