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
 
package io.onetable.persistence;

import static io.onetable.constants.OneTableConstants.ONETABLE_META_DIR;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.onetable.model.OneTable;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.stat.Range;
import io.onetable.model.storage.DataLayoutStrategy;
import io.onetable.model.storage.FileFormat;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.TableFormat;
import io.onetable.model.sync.SyncMode;
import io.onetable.model.sync.SyncResult;
import io.onetable.schema.SchemaFieldFinder;

class TestOneTableStateStorage {
  @TempDir public static Path tempDir;

  private static OneTable oneTable;
  private static OneTableStateStorage storage;

  private static final ObjectMapper MAPPER =
      new ObjectMapper().findAndRegisterModules().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
  private static final Map<OneSchema.MetadataKey, Object> TIMESTAMP_MILLIS_METADATA =
      ImmutableMap.<OneSchema.MetadataKey, Object>builder()
          .put(OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MILLIS)
          .build();
  private static final OneSchema ONE_SCHEMA =
      OneSchema.builder()
          .dataType(OneType.RECORD)
          .name("parent")
          .fields(
              Arrays.asList(
                  OneField.builder()
                      .name("timestamp_field")
                      .schema(
                          OneSchema.builder()
                              .name("long")
                              .dataType(OneType.TIMESTAMP_NTZ)
                              .metadata(TIMESTAMP_MILLIS_METADATA)
                              .build())
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
                      .name("float_field")
                      .schema(OneSchema.builder().name("float").dataType(OneType.FLOAT).build())
                      .build(),
                  OneField.builder()
                      .name("double_field")
                      .schema(OneSchema.builder().name("float").dataType(OneType.DOUBLE).build())
                      .build(),
                  OneField.builder()
                      .name("boolean_field")
                      .schema(OneSchema.builder().name("boolean").dataType(OneType.BOOLEAN).build())
                      .build(),
                  OneField.builder()
                      .name("bytes_field")
                      .schema(OneSchema.builder().name("bytes").dataType(OneType.BYTES).build())
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

  private static final OnePartitionField PARTITION_FIELD =
      OnePartitionField.builder()
          .sourceField(
              SchemaFieldFinder.getInstance().findFieldByPath(ONE_SCHEMA, "timestamp_field"))
          .transformType(PartitionTransformType.DAY)
          .build();

  private static final Instant START_TIME = Instant.ofEpochSecond(1666719036);
  private static final Instant END_TIME = START_TIME.plus(10, ChronoUnit.SECONDS);
  private static final Instant LAST_COMMIT = START_TIME.minus(1, ChronoUnit.HOURS);
  private static final String TABLE_NAME = "testtimestamppartitionfiltering";
  private static final Random RANDOM = new Random();
  private static Path basePath = null;
  private static final SyncResult ERR_SYNC_RESULT =
      SyncResult.builder()
          .mode(SyncMode.FULL)
          .status(
              SyncResult.SyncStatus.builder()
                  .statusCode(SyncResult.SyncStatusCode.ERROR)
                  .errorMessage("generic error message")
                  .errorDescription(
                      "Failed to sync snapshot to Iceberg. Could not create HadoopCatalog")
                  .canRetryOnFailure(true)
                  .build())
          .syncStartTime(START_TIME)
          .syncDuration(Duration.between(START_TIME, END_TIME))
          .lastInstantSynced(LAST_COMMIT)
          .build();

  @BeforeAll
  public static void setupOnce() {
    Configuration configuration = new Configuration();
    configuration.set("parquet.avro.write-old-list-structure", "false");
    storage = new OneTableStateStorage(configuration, true);
    basePath = tempDir.resolve(TABLE_NAME);
    oneTable =
        OneTable.builder()
            .name(TABLE_NAME)
            .basePath(basePath.toString())
            .layoutStrategy(DataLayoutStrategy.FLAT)
            .tableFormat(TableFormat.HUDI)
            .readSchema(ONE_SCHEMA)
            .partitioningFields(Collections.singletonList(PARTITION_FIELD))
            .build();
  }

  @Test
  void testWriteAndRead() throws IOException {
    SyncResult syncResult =
        SyncResult.builder()
            .mode(SyncMode.FULL)
            .status(
                SyncResult.SyncStatus.builder()
                    .statusCode(SyncResult.SyncStatusCode.SUCCESS)
                    .build())
            .syncStartTime(START_TIME)
            .syncDuration(Duration.between(START_TIME, END_TIME))
            .lastInstantSynced(LAST_COMMIT)
            .lastSyncedDataFiles(getLatestSyncState())
            .build();

    OneTableState state =
        OneTableState.builder()
            .table(oneTable)
            .tableFormatsToSync(ImmutableList.of(TableFormat.ICEBERG, TableFormat.DELTA))
            .lastSyncResult(
                ImmutableMap.of(
                    TableFormat.DELTA, syncResult,
                    TableFormat.ICEBERG, ERR_SYNC_RESULT))
            .build();
    storage.write(state);

    Path basePath = tempDir.resolve(TABLE_NAME);
    OneTableState stateFromStorage = storage.read(basePath.toString()).get();
    assertEquals(state, stateFromStorage);

    // Write same state again to ensure archived results can be parsed.
    storage.write(state);
    stateFromStorage = storage.read(basePath.toString()).get();
    assertEquals(state, stateFromStorage);

    for (TableFormat tableFormat : TableFormat.values()) {
      org.apache.hadoop.fs.Path syncArchivedFilePath =
          new org.apache.hadoop.fs.Path(
              String.format(
                  "%s/%s/%s.sync.archive",
                  basePath, ONETABLE_META_DIR, tableFormat.name().toLowerCase(Locale.ROOT)));
      try (FileSystem fs = syncArchivedFilePath.getFileSystem(new Configuration())) {
        List<SyncResult> syncResultList = new ArrayList<>();
        if (fs.exists(syncArchivedFilePath)) {
          try (FSDataInputStream fsDataInputStream = fs.open(syncArchivedFilePath);
              BufferedReader bufferedReader =
                  new BufferedReader(
                      new InputStreamReader(fsDataInputStream, StandardCharsets.UTF_8))) {
            String readLine = bufferedReader.readLine();
            while (readLine != null) {
              syncResultList.add(MAPPER.readValue(readLine, SyncResult.class));
              readLine = bufferedReader.readLine();
            }
          }
          assertEquals(syncResultList.size(), 2);
        }
      }
    }
  }

  private OneDataFile getOneDataFile(
      int index, Map<OnePartitionField, Range> partitionValues, Path basePath) {
    String physicalPath =
        new org.apache.hadoop.fs.Path(basePath.toUri() + "physical" + index + ".parquet")
            .toString();
    return OneDataFile.builder()
        .fileFormat(FileFormat.APACHE_PARQUET)
        .fileSizeBytes(RANDOM.nextInt(10000))
        .physicalPath(physicalPath)
        .recordCount(RANDOM.nextInt(10000))
        .partitionValues(partitionValues)
        .columnStats(getColumnStats())
        .build();
  }

  private Map<OneField, ColumnStat> getColumnStats() {
    Map<OneField, ColumnStat> columnStatMap = new HashMap<>();
    columnStatMap.put(
        SchemaFieldFinder.getInstance().findFieldByPath(ONE_SCHEMA, "float_field"),
        ColumnStat.builder().numNulls(0).totalSize(123).range(Range.vector(1.2f, 2.5f)).build());
    columnStatMap.put(
        SchemaFieldFinder.getInstance().findFieldByPath(ONE_SCHEMA, "group_id"),
        ColumnStat.builder().numNulls(0).totalSize(123).range(Range.vector(1, 10)).build());
    columnStatMap.put(
        SchemaFieldFinder.getInstance().findFieldByPath(ONE_SCHEMA, "record.string_field"),
        ColumnStat.builder().numNulls(1).totalSize(456).range(Range.vector("a", "b")).build());
    columnStatMap.put(
        SchemaFieldFinder.getInstance().findFieldByPath(ONE_SCHEMA, "timestamp_field"),
        ColumnStat.builder().numNulls(0).totalSize(123).range(Range.vector(123L, 987L)).build());
    columnStatMap.put(
        SchemaFieldFinder.getInstance().findFieldByPath(ONE_SCHEMA, "double_field"),
        ColumnStat.builder().numNulls(0).totalSize(123).range(Range.vector(2.0, 4.0)).build());
    columnStatMap.put(
        SchemaFieldFinder.getInstance().findFieldByPath(ONE_SCHEMA, "boolean_field"),
        ColumnStat.builder().numNulls(0).totalSize(2).range(Range.vector(false, true)).build());
    columnStatMap.put(
        SchemaFieldFinder.getInstance().findFieldByPath(ONE_SCHEMA, "bytes_field"),
        ColumnStat.builder()
            .numNulls(50)
            .totalSize(123998439)
            .range(
                Range.vector(
                    ByteBuffer.wrap("start_bytes".getBytes(StandardCharsets.UTF_8)),
                    ByteBuffer.wrap("end_bytes".getBytes(StandardCharsets.UTF_8))))
            .build());
    return columnStatMap;
  }

  private OneDataFiles getLatestSyncState() {
    OnePartitionField onePartitionField =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("string_field")
                    .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    Map<OnePartitionField, Range> partitionValues1 = new HashMap<>();
    partitionValues1.put(onePartitionField, Range.scalar("level"));
    Map<OnePartitionField, Range> partitionValues2 = new HashMap<>();
    partitionValues2.put(onePartitionField, Range.scalar("warning"));
    OneDataFile dataFile1 = getOneDataFile(1, partitionValues1, basePath);
    OneDataFile dataFile2 = getOneDataFile(2, partitionValues1, basePath);
    OneDataFile dataFile3 = getOneDataFile(3, partitionValues2, basePath);
    OneDataFiles group1 =
        OneDataFiles.collectionBuilder()
            .files(Arrays.asList(dataFile1, dataFile2))
            .partitionPath("base/level")
            .build();
    OneDataFiles group2 =
        OneDataFiles.collectionBuilder()
            .files(Arrays.asList(dataFile3))
            .partitionPath("base/warning")
            .build();
    return OneDataFiles.collectionBuilder().files(Arrays.asList(group1, group2)).build();
  }
}
