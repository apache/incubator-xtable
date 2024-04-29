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
 
package org.apache.xtable;

import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME;
import static org.apache.xtable.hudi.HudiTestUtil.getHoodieWriteConfig;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.SneakyThrows;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTimelineTimeZone;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.CustomKeyGenerator;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.TimestampBasedKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.metadata.HoodieMetadataFileSystemView;

import com.google.common.base.Preconditions;

public abstract class TestAbstractHudiTable
    implements GenericTable<HoodieRecord<HoodieAvroPayload>, String> {

  static {
    // ensure json modules are registered before any json serialization/deserialization
    JsonUtils.registerModules();
  }

  protected static final String RECORD_KEY_FIELD_NAME = "key";
  protected static final Schema BASIC_SCHEMA;

  private static final Random RANDOM = new Random();

  static {
    try (InputStream schemaStream =
        TestAbstractHudiTable.class
            .getClassLoader()
            .getResourceAsStream("schemas/basic_schema.avsc")) {
      BASIC_SCHEMA = new Schema.Parser().parse(schemaStream);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }
  // Name of the table
  protected String tableName;
  // Base path for the table
  protected String basePath;
  protected HoodieTableMetaClient metaClient;
  protected TypedProperties typedProperties;
  protected KeyGenerator keyGenerator;
  protected Schema schema;
  protected List<String> partitionFieldNames;

  TestAbstractHudiTable(String name, Schema schema, Path tempDir, String partitionConfig) {
    try {
      this.tableName = name;
      this.schema = schema;
      // Initialize base path
      this.basePath = initBasePath(tempDir, name);
      // Add key generator
      this.typedProperties = new TypedProperties();
      typedProperties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), RECORD_KEY_FIELD_NAME);
      if (partitionConfig == null) {
        this.keyGenerator = new NonpartitionedKeyGenerator(typedProperties);
        this.partitionFieldNames = Collections.emptyList();
      } else {
        if (partitionConfig.contains("timestamp")) {
          typedProperties.put("hoodie.keygen.timebased.timestamp.type", "SCALAR");
          typedProperties.put("hoodie.keygen.timebased.timestamp.scalar.time.unit", "MICROSECONDS");
          typedProperties.put("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd");
          typedProperties.put("hoodie.keygen.timebased.input.timezone", "UTC");
          typedProperties.put("hoodie.keygen.timebased.output.timezone", "UTC");
        }
        String[] partitionFieldConfigs = partitionConfig.split(",");
        if (partitionFieldConfigs.length == 1 && !partitionFieldConfigs[0].contains(".")) {
          typedProperties.put(
              PARTITIONPATH_FIELD_NAME.key(), partitionFieldConfigs[0].split(":")[0]);
          if (partitionFieldConfigs[0].contains(".")) { // nested field
            this.keyGenerator = new CustomKeyGenerator(typedProperties);
          } else if (partitionFieldConfigs[0].contains("SIMPLE")) { // top level field
            this.keyGenerator = new SimpleKeyGenerator(typedProperties);
          } else { // top level timestamp field
            typedProperties.put(PARTITIONPATH_FIELD_NAME.key(), partitionConfig);
            this.keyGenerator = new TimestampBasedKeyGenerator(typedProperties);
          }
        } else {
          typedProperties.put(PARTITIONPATH_FIELD_NAME.key(), partitionConfig);
          this.keyGenerator = new CustomKeyGenerator(typedProperties);
        }
        this.partitionFieldNames =
            Arrays.stream(partitionFieldConfigs)
                .map(config -> config.split(":")[0])
                .collect(Collectors.toList());
      }
    } catch (IOException ex) {
      throw new UncheckedIOException("Unable to initialize Test Hudi Table", ex);
    }
  }

  protected abstract BaseHoodieWriteClient<?, ?, ?, ?> getWriteClient();

  public String getBasePath() {
    return basePath;
  }

  protected HoodieRecord<HoodieAvroPayload> getRecord(
      Schema schema,
      String key,
      Instant timeLowerBound,
      Instant timeUpperBound,
      GenericRecord existingRecord,
      Object partitionValue) {
    GenericRecord record =
        generateGenericRecord(
            schema, key, timeLowerBound, timeUpperBound, existingRecord, partitionValue);
    HoodieKey hoodieKey = keyGenerator.getKey(record);
    return new HoodieAvroRecord<>(hoodieKey, new HoodieAvroPayload(Option.of(record)));
  }

  public abstract List<HoodieRecord<HoodieAvroPayload>> insertRecords(
      int numRecords, boolean checkForNoErrors);

  public abstract List<HoodieRecord<HoodieAvroPayload>> insertRecords(
      int numRecords, Object partitionValue, boolean checkForNoErrors);

  public List<HoodieRecord<HoodieAvroPayload>> generateRecords(int numRecords) {
    return generateRecords(numRecords, null);
  }

  public List<HoodieRecord<HoodieAvroPayload>> generateRecords(
      int numRecords, Object partitionValue) {
    Preconditions.checkArgument(
        partitionValue == null || !partitionFieldNames.isEmpty(),
        "To generate records for a specific partition, table has to be partitioned.");
    Instant currentTime = Instant.now().truncatedTo(ChronoUnit.DAYS);
    List<Instant> startTimeWindows =
        Arrays.asList(
            currentTime.minus(2, ChronoUnit.DAYS),
            currentTime.minus(3, ChronoUnit.DAYS),
            currentTime.minus(4, ChronoUnit.DAYS));
    List<Instant> endTimeWindows =
        Arrays.asList(
            currentTime.minus(1, ChronoUnit.DAYS),
            currentTime.minus(2, ChronoUnit.DAYS),
            currentTime.minus(3, ChronoUnit.DAYS));
    List<HoodieRecord<HoodieAvroPayload>> inserts =
        IntStream.range(0, numRecords)
            .mapToObj(
                index ->
                    getRecord(
                        schema,
                        UUID.randomUUID().toString(),
                        startTimeWindows.get(index % 3),
                        endTimeWindows.get(index % 3),
                        null,
                        partitionValue))
            .collect(Collectors.toList());
    return inserts;
  }

  public String startCommit() {
    return getStartCommitInstant();
  }

  protected String getStartCommitInstant() {
    return getWriteClient().startCommit(metaClient.getCommitActionType(), metaClient);
  }

  protected String getStartCommitOfActionType(String actionType) {
    return getWriteClient().startCommit(actionType, metaClient);
  }

  public List<HoodieRecord<HoodieAvroPayload>> insertRecords(
      boolean checkForNoErrors, List<HoodieRecord<HoodieAvroPayload>> inserts) {
    String instant = getStartCommitInstant();
    return insertRecordsWithCommitAlreadyStarted(inserts, instant, checkForNoErrors);
  }

  public abstract List<HoodieRecord<HoodieAvroPayload>> insertRecordsWithCommitAlreadyStarted(
      List<HoodieRecord<HoodieAvroPayload>> inserts,
      String commitInstant,
      boolean checkForNoErrors);

  public abstract List<HoodieRecord<HoodieAvroPayload>> upsertRecordsWithCommitAlreadyStarted(
      List<HoodieRecord<HoodieAvroPayload>> records,
      String commitInstant,
      boolean checkForNoErrors);

  public List<HoodieRecord<HoodieAvroPayload>> upsertRecords(
      List<HoodieRecord<HoodieAvroPayload>> records, boolean checkForNoErrors) {
    String instant = getStartCommitInstant();
    return upsertRecordsWithCommitAlreadyStarted(records, instant, checkForNoErrors);
  }

  public abstract List<HoodieKey> deleteRecords(
      List<HoodieRecord<HoodieAvroPayload>> records, boolean checkForNoErrors);

  public abstract void deletePartition(String partition, HoodieTableType tableType);

  public abstract void cluster();

  public List<String> getAllLatestBaseFilePaths() {
    HoodieTableFileSystemView fsView =
        new HoodieMetadataFileSystemView(
            getWriteClient().getEngineContext(),
            metaClient,
            metaClient.reloadActiveTimeline(),
            getHoodieWriteConfig(metaClient).getMetadataConfig());
    return getAllLatestBaseFiles(fsView).stream()
        .map(HoodieBaseFile::getPath)
        .collect(Collectors.toList());
  }

  public void compact() {
    String instant = onlyScheduleCompaction();
    getWriteClient().compact(instant);
  }

  public String onlyScheduleCompaction() {
    return getWriteClient().scheduleCompaction(Option.empty()).get();
  }

  public void completeScheduledCompaction(String instant) {
    getWriteClient().compact(instant);
  }

  public void clean() {
    HoodieCleanMetadata metadata = getWriteClient().clean();
    // Assert that files are deleted to ensure test is realistic
    assertTrue(metadata.getTotalFilesDeleted() > 0);
  }

  public void rollback(String commitInstant) {
    getWriteClient().rollback(commitInstant);
  }

  /**
   * Restore to nth latest instant. 1 means restore to the previous instant, 2 would be the instant
   * before that, and so on. 0 would mean restore to the latest instant which doesn't make sense to
   * do.
   *
   * @param n instant to restore from
   */
  public void savepointRestoreFromNthMostRecentInstant(int n) {
    List<HoodieInstant> commitInstants =
        metaClient.getActiveTimeline().reload().getCommitsTimeline().getInstants();
    HoodieInstant instantToRestore = commitInstants.get(commitInstants.size() - 1 - n);
    getWriteClient().savepoint(instantToRestore.getTimestamp(), "user", "savepoint-test");
    getWriteClient().restoreToSavepoint(instantToRestore.getTimestamp());
    assertMergeOnReadRestoreContainsLogFiles();
  }

  public List<HoodieBaseFile> getAllLatestBaseFiles(HoodieTableFileSystemView fsView) {
    try {
      fsView.loadAllPartitions();
      return fsView.getLatestBaseFiles().collect(Collectors.toList());
    } finally {
      fsView.close();
    }
  }

  public void assertMergeOnReadRestoreContainsLogFiles() {
    if (metaClient.getTableConfig().getTableType() == HoodieTableType.MERGE_ON_READ) {
      HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline().reload();
      HoodieInstant restoreInstant = activeTimeline.getRestoreTimeline().firstInstant().get();
      Option<byte[]> instantDetails = activeTimeline.getInstantDetails(restoreInstant);
      try {
        HoodieRestoreMetadata instantMetadata =
            TimelineMetadataUtils.deserializeAvroMetadata(
                instantDetails.get(), HoodieRestoreMetadata.class);
        assertTrue(
            instantMetadata.getHoodieRestoreMetadata().values().stream()
                .flatMap(
                    list ->
                        list.stream()
                            .flatMap(
                                rollbackMetadata ->
                                    rollbackMetadata.getPartitionMetadata().values().stream()
                                        .flatMap(
                                            partitionMetadata ->
                                                partitionMetadata
                                                    .getSuccessDeleteFiles()
                                                    .stream())))
                .map(uri -> FSUtils.isLogFile(new org.apache.hadoop.fs.Path(uri).getName()))
                .reduce((a, b) -> a || b)
                .get());
      } catch (IOException e) {
        throw new UncheckedIOException("Cannot deserialize restore metadata", e);
      }
    }
  }

  public static void assertNoWriteErrors(List<WriteStatus> statuses) {
    assertAll(
        statuses.stream()
            .map(
                status ->
                    () ->
                        assertFalse(
                            status.hasErrors(), "Errors found in write of " + status.getFileId())));
  }

  public List<HoodieRecord<HoodieAvroPayload>> generateUpdatesForRecords(
      List<HoodieRecord<HoodieAvroPayload>> records) {
    Instant startTimeWindow = Instant.now().truncatedTo(ChronoUnit.DAYS).minus(1, ChronoUnit.DAYS);
    Instant endTimeWindow = Instant.now().truncatedTo(ChronoUnit.DAYS);
    return records.stream()
        .map(
            existingRecord -> {
              try {
                return getRecord(
                    schema,
                    existingRecord.getRecordKey(),
                    startTimeWindow,
                    endTimeWindow,
                    (GenericRecord) (existingRecord.getData()).getInsertValue(schema).get(),
                    null);
              } catch (IOException ex) {
                throw new UncheckedIOException(ex);
              }
            })
        .collect(Collectors.toList());
  }

  protected HoodieWriteConfig generateWriteConfig(Schema schema, TypedProperties keyGenProperties) {
    // allow for compaction and cleaning after a single commit for testing different timeline
    // scenarios
    HoodieCompactionConfig compactionConfig =
        HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1).build();
    HoodieClusteringConfig clusteringConfig =
        HoodieClusteringConfig.newBuilder().withClusteringSortColumns("long_field").build();
    HoodieCleanConfig cleanConfig =
        HoodieCleanConfig.newBuilder()
            .retainCommits(1)
            .withMaxCommitsBeforeCleaning(1)
            .withAutoClean(false)
            .build();
    HoodieStorageConfig storageConfig =
        HoodieStorageConfig.newBuilder().parquetCompressionCodec("UNCOMPRESSED").build();
    HoodieArchivalConfig archivalConfig =
        HoodieArchivalConfig.newBuilder().archiveCommitsWith(3, 4).build();
    HoodieMetadataConfig metadataConfig =
        HoodieMetadataConfig.newBuilder()
            .enable(true)
            // enable col stats only on un-partitioned data due to bug in Hudi
            // https://issues.apache.org/jira/browse/HUDI-6954
            .withMetadataIndexColumnStats(
                !keyGenProperties.getString(PARTITIONPATH_FIELD_NAME.key(), "").isEmpty())
            .withColumnStatsIndexForColumns(getColumnsFromSchema(schema))
            .build();
    Properties lockProperties = new Properties();
    lockProperties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    lockProperties.setProperty(
        LockConfiguration.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "3000");
    lockProperties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY, "20");
    return HoodieWriteConfig.newBuilder()
        .withProperties(keyGenProperties)
        .withPath(this.basePath)
        .withSchema(schema.toString())
        .withKeyGenerator(keyGenerator.getClass().getCanonicalName())
        .withCompactionConfig(compactionConfig)
        .withClusteringConfig(clusteringConfig)
        .withCleanConfig(cleanConfig)
        .withArchivalConfig(archivalConfig)
        .withMetadataConfig(metadataConfig)
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withMarkersType(MarkerType.DIRECT.name())
        .withStorageConfig(storageConfig)
        .withLockConfig(
            HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build())
        .withProperties(lockProperties)
        .build();
  }

  /**
   * Creates a comma separated list of all the columns in dot notation so that all columns will have
   * stats saved to metadata table for more thorough testing.
   *
   * @param schema the writer schema
   * @return comma separated list of all columns in dot notation
   */
  private String getColumnsFromSchema(Schema schema) {
    Queue<Pair<String, Schema.Field>> parentAndField =
        new ArrayDeque<>(
            schema.getFields().stream()
                .map(field -> Pair.of("", field))
                .collect(Collectors.toList()));
    String output = "";
    while (!parentAndField.isEmpty()) {
      Pair<String, Schema.Field> pair = parentAndField.poll();
      String parent = pair.getKey();
      Schema.Field field = pair.getValue();
      String fieldName = field.name();
      Schema fieldSchema = unwrapNullableSchema(field.schema());
      String prefix = parent.isEmpty() ? "" : parent + ".";
      if (fieldSchema.getType() == Schema.Type.RECORD) {
        fieldSchema.getFields().stream()
            .map(subField -> Pair.of(prefix + fieldName, subField))
            .forEach(parentAndField::add);
      } else if (fieldSchema.getType() == Schema.Type.ARRAY) {
        parentAndField.add(
            Pair.of(
                prefix + fieldName + ".list",
                new Schema.Field("element", fieldSchema.getElementType(), "", null)));
      } else if (fieldSchema.getType() == Schema.Type.MAP) {
        output += prefix + fieldName + ".key_value.key" + ",";
        parentAndField.add(
            Pair.of(
                prefix + fieldName + ".key_value",
                new Schema.Field("value", fieldSchema.getValueType(), "", null)));
      } else {
        output += prefix + fieldName + ",";
      }
    }
    return output;
  }

  private Schema unwrapNullableSchema(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      return schema.getTypes().get(0).getType() == Schema.Type.NULL
          ? schema.getTypes().get(1)
          : schema.getTypes().get(0);
    }
    return schema;
  }

  // Create the base path and store it for reference
  protected String initBasePath(Path tempDir, String tableName) throws IOException {
    // make sure that table name in hudi is not coupled to path
    Path basePath = tempDir.resolve(tableName + "_v1");
    Files.createDirectories(basePath);
    return basePath.toUri().toString();
  }

  protected static Schema addSchemaEvolutionFieldsToBase(Schema schema) {
    Schema nestedRecordSchema = schema.getField("nested_record").schema().getTypes().get(1);
    List<Schema.Field> newNestedRecordFields = new ArrayList<>();
    for (Schema.Field existingNestedRecordField : nestedRecordSchema.getFields()) {
      newNestedRecordFields.add(copyField(existingNestedRecordField));
    }
    Schema nullableStringSchema =
        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
    newNestedRecordFields.add(
        new Schema.Field("new_nested_field", nullableStringSchema, "doc", null));
    Schema newNestedRecordSchema =
        Schema.createRecord(
            nestedRecordSchema.getName(),
            nestedRecordSchema.getDoc(),
            nestedRecordSchema.getNamespace(),
            false,
            newNestedRecordFields);

    List<Schema.Field> newFields = new ArrayList<>();
    for (Schema.Field existingField : schema.getFields()) {
      // update existing instances of nested_record
      if (existingField.name().equals("nested_record")) {
        newFields.add(
            new Schema.Field(
                existingField.name(),
                Schema.createUnion(Schema.create(Schema.Type.NULL), newNestedRecordSchema),
                existingField.doc(),
                existingField.defaultVal()));
      } else if (existingField.name().equals("nullable_map_field")) {
        newFields.add(
            new Schema.Field(
                existingField.name(),
                Schema.createUnion(
                    Schema.create(Schema.Type.NULL), Schema.createMap(newNestedRecordSchema)),
                existingField.doc(),
                existingField.defaultVal()));
      } else if (existingField.name().equals("array_field")) {
        newFields.add(
            new Schema.Field(
                existingField.name(),
                Schema.createArray(newNestedRecordSchema),
                existingField.doc(),
                existingField.defaultVal()));
      } else {
        newFields.add(copyField(existingField));
      }
    }
    return Schema.createRecord(
        schema.getName(), schema.getDoc(), schema.getNamespace(), false, newFields);
  }

  protected static Schema addTopLevelField(Schema schema) {
    Schema nullableStringSchema =
        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));

    List<Schema.Field> newFields =
        new ArrayList<>(
            schema.getFields().stream()
                .map(TestAbstractHudiTable::copyField)
                .collect(Collectors.toList()));
    newFields.add(new Schema.Field("new_top_level_field", nullableStringSchema, "", null));
    return Schema.createRecord(
        schema.getName(), schema.getDoc(), schema.getNamespace(), false, newFields);
  }

  @SneakyThrows
  protected HoodieTableMetaClient getMetaClient(
      TypedProperties keyGenProperties, HoodieTableType hoodieTableType, Configuration conf) {
    LocalFileSystem fs = (LocalFileSystem) FSUtils.getFs(basePath, conf);
    // Enforce checksum such that fs.open() is consistent to DFS
    fs.setVerifyChecksum(true);
    fs.mkdirs(new org.apache.hadoop.fs.Path(basePath));

    if (fs.exists(new org.apache.hadoop.fs.Path(basePath + "/.hoodie"))) {
      return HoodieTableMetaClient.builder()
          .setConf(conf)
          .setBasePath(basePath)
          .setLoadActiveTimelineOnLoad(true)
          .build();
    }
    Properties properties =
        HoodieTableMetaClient.withPropertyBuilder()
            .fromProperties(keyGenProperties)
            .setTableName(tableName)
            .setTableType(hoodieTableType)
            .setKeyGeneratorClassProp(keyGenerator.getClass().getCanonicalName())
            .setPartitionFields(String.join(",", partitionFieldNames))
            .setRecordKeyFields(RECORD_KEY_FIELD_NAME)
            .setPayloadClass(OverwriteWithLatestAvroPayload.class)
            .setCommitTimezone(HoodieTimelineTimeZone.UTC)
            .setBaseFileFormat(HoodieFileFormat.PARQUET.toString())
            .build();
    return HoodieTableMetaClient.initTableAndGetMetaClient(conf, this.basePath, properties);
  }

  private static Schema.Field copyField(Schema.Field input) {
    return new Schema.Field(input.name(), input.schema(), input.doc(), input.defaultVal());
  }

  private GenericRecord generateGenericRecord(
      Schema schema,
      String key,
      Instant timeLowerBound,
      Instant timeUpperBound,
      GenericRecord existingRecord,
      Object partitionValue) {
    GenericRecord record = new GenericData.Record(schema);
    for (Schema.Field field : schema.getFields()) {
      Object value;
      String fieldName = field.name();
      Schema fieldSchema =
          field.schema().getType() == Schema.Type.UNION
              ? field.schema().getTypes().get(1)
              : field.schema();
      if (existingRecord != null && partitionFieldNames.contains(fieldName)) {
        // Leave existing partition values
        value = existingRecord.get(fieldName);
      } else if (partitionValue != null && partitionFieldNames.contains(fieldName)) {
        value = partitionValue;
      } else if (fieldName.equals(RECORD_KEY_FIELD_NAME)) {
        // set key to the provided value
        value = key;
      } else if (fieldName.equals("ts")) {
        // always set ts to current time for update ordering
        value = System.currentTimeMillis();
      } else if (fieldName.equals("level")) {
        // a simple string field to be used for basic partitioning if required
        value = LEVEL_VALUES.get(RANDOM.nextInt(LEVEL_VALUES.size()));
      } else if (fieldName.equals("severity")) {
        // a bounded integer field to be used for partition testing
        value = RANDOM.nextBoolean() ? null : RANDOM.nextInt(3);
      } else if (fieldName.startsWith("time")) {
        // limit time fields to particular windows for the sake of testing time based partitions
        long timeWindow = timeUpperBound.toEpochMilli() - timeLowerBound.toEpochMilli();
        LogicalType logicalType = fieldSchema.getLogicalType();
        if (logicalType instanceof LogicalTypes.TimestampMillis
            || logicalType instanceof LogicalTypes.LocalTimestampMillis) {
          value = timeLowerBound.plusMillis(RANDOM.nextInt((int) timeWindow)).toEpochMilli();
        } else if (logicalType instanceof LogicalTypes.TimestampMicros
            || logicalType instanceof LogicalTypes.LocalTimestampMicros) {
          value = timeLowerBound.plusMillis(RANDOM.nextInt((int) timeWindow)).toEpochMilli() * 1000;
        } else {
          throw new IllegalArgumentException(
              "Unhandled timestamp type: " + fieldSchema.getLogicalType());
        }
      } else if (fieldName.startsWith("date")) {
        value = (int) timeLowerBound.atZone(ZoneId.of("UTC")).toLocalDate().toEpochDay();
      } else if (field.schema().isNullable() && RANDOM.nextBoolean()) {
        // set the value to null to help generate interesting col stats and test null handling
        value = null;
      } else {
        Schema.Type fieldType = fieldSchema.getType();
        switch (fieldType) {
          case FLOAT:
            value = RANDOM.nextFloat();
            break;
          case DOUBLE:
            value = RANDOM.nextDouble();
            break;
          case LONG:
            value = RANDOM.nextLong();
            break;
          case INT:
            value = RANDOM.nextInt();
            break;
          case BOOLEAN:
            value = RANDOM.nextBoolean();
            break;
          case STRING:
            value = RandomStringUtils.randomAlphabetic(10);
            break;
          case BYTES:
            value =
                ByteBuffer.wrap(
                    RandomStringUtils.randomAlphabetic(10).getBytes(StandardCharsets.UTF_8));
            break;
          case FIXED:
            if (fieldSchema.getLogicalType() != null
                && fieldSchema.getLogicalType() instanceof LogicalTypes.Decimal) {
              value = BigDecimal.valueOf(RANDOM.nextInt(1000000), 2);
            } else {
              value =
                  new GenericData.Fixed(
                      fieldSchema,
                      RandomStringUtils.randomAlphabetic(10).getBytes(StandardCharsets.UTF_8));
            }
            break;
          case ENUM:
            Schema enumSchema = field.schema();
            value =
                new GenericData.EnumSymbol(
                    enumSchema,
                    enumSchema
                        .getEnumSymbols()
                        .get(RANDOM.nextInt(enumSchema.getEnumSymbols().size())));
            break;
          case RECORD:
            value =
                generateGenericRecord(
                    fieldSchema,
                    key,
                    timeLowerBound,
                    timeUpperBound,
                    existingRecord == null ? null : (GenericRecord) existingRecord.get(fieldName),
                    partitionValue);
            break;
          case ARRAY:
            value =
                IntStream.range(0, RANDOM.nextInt(2) + 1)
                    .mapToObj(
                        unused ->
                            generateGenericRecord(
                                fieldSchema.getElementType(),
                                key,
                                timeLowerBound,
                                timeUpperBound,
                                null,
                                null))
                    .collect(Collectors.toList());
            break;
          case MAP:
            value =
                IntStream.range(0, RANDOM.nextInt(2) + 1)
                    .mapToObj(
                        unused ->
                            generateGenericRecord(
                                fieldSchema.getValueType(),
                                key,
                                timeLowerBound,
                                timeUpperBound,
                                null,
                                null))
                    .collect(
                        Collectors.toMap(
                            unused -> RandomStringUtils.randomAlphabetic(5), Function.identity()));
            break;
          default:
            throw new UnsupportedOperationException(
                "Field type not properly handle in data generation: " + fieldType);
        }
      }
      record.put(fieldName, value);
    }
    return record;
  }

  @Override
  public List<HoodieRecord<HoodieAvroPayload>> insertRows(int numRecords) {
    List<HoodieRecord<HoodieAvroPayload>> inserts = generateRecords(numRecords);
    return insertRecords(true, inserts);
  }

  @Override
  public void upsertRows(List<HoodieRecord<HoodieAvroPayload>> records) {
    String instant = getStartCommitInstant();
    upsertRecordsWithCommitAlreadyStarted(records, instant, true);
  }

  @Override
  public List<HoodieRecord<HoodieAvroPayload>> insertRecordsForSpecialPartition(int numRecords) {
    return insertRecords(numRecords, SPECIAL_PARTITION_VALUE, true);
  }

  @Override
  public List<String> getColumnsToSelect() {
    return schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
  }

  @Override
  public void deleteRows(List<HoodieRecord<HoodieAvroPayload>> records) {
    deleteRecords(records, true);
  }

  @Override
  public void reload() {
    // no-op.
  }

  @Override
  public String getFilterQuery() {
    return String.format("%s > 'aaa'", RECORD_KEY_FIELD_NAME);
  }

  @Override
  public String getOrderByColumn() {
    return "_hoodie_record_key";
  }
}
