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
 
package io.onetable;

import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.Closeable;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Assertions;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTimelineTimeZone;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.CustomKeyGenerator;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

public class TestHudiTable implements Closeable {
  private static final Random RANDOM = new Random();
  // A list of values for the level field which serves as a basic field to partition on for tests
  private static final List<String> LEVEL_VALUES = Arrays.asList("INFO", "WARN", "ERROR");
  private static final String RECORD_KEY_FIELD_NAME = "key";
  private static final Schema BASIC_SCHEMA;

  static {
    try (InputStream schemaStream =
        TestHudiTable.class.getClassLoader().getResourceAsStream("schemas/basic_schema.avsc")) {
      BASIC_SCHEMA = new Schema.Parser().parse(schemaStream);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }
  // Name of the table
  private final String tableName;
  // Base path for the table
  private final String basePath;
  private final JavaSparkContext jsc;
  private final HoodieTableMetaClient metaClient;
  private final SparkRDDWriteClient<HoodieAvroPayload> sparkWriteClient;
  private final HoodieJavaWriteClient<HoodieAvroPayload> javaWriteClient;
  // true if the table should use the spark client for its read & write operations.
  private final boolean tableUsesSparkClient;
  private final KeyGenerator keyGenerator;
  private final Schema schema;
  private final List<String> partitionFieldNames;

  /**
   * Create a test table instance for general testing. The table is created with the schema defined
   * in basic_schema.avsc which contains many data types to ensure they are handled correctly.
   *
   * @param tableName name of the table used in the test, should be unique per test within a shared
   *     directory
   * @param tempDir directory where table will be written, typically a temporary directory that will
   *     be cleaned up after the tests.
   * @param jsc the {@link JavaSparkContext} to use when writing data with Hudi
   * @param partitionConfig sets the property `hoodie.datasource.write.partitionpath.field` for the
   *     {@link CustomKeyGenerator}. If null, {@link NonpartitionedKeyGenerator} will be used.
   * @param tableType the table type to use (MoR or CoW)
   * @return an instance of the class with this configuration
   */
  public static TestHudiTable forStandardSchema(
      String tableName,
      Path tempDir,
      JavaSparkContext jsc,
      String partitionConfig,
      HoodieTableType tableType,
      boolean tableUsesSparkClient) {
    return new TestHudiTable(
        tableName, BASIC_SCHEMA, tempDir, jsc, partitionConfig, tableType, tableUsesSparkClient);
  }

  /**
   * Create a test table instance with a schema that has more fields than an instance returned by
   * {@link #forStandardSchema(String, Path, JavaSparkContext, String, HoodieTableType, boolean)}.
   * Specifically this instance will add a top level field, nested field, field within a list, and
   * field within a map to ensure schema evolution is properly handled.
   *
   * @param tableName name of the table used in the test, should be unique per test within a shared
   *     directory
   * @param tempDir directory where table will be written, typically a temporary directory that will
   *     be cleaned up after the tests.
   * @param jsc the {@link JavaSparkContext} to use when writing data with Hudi
   * @param partitionConfig sets the property `hoodie.datasource.write.partitionpath.field` for the
   *     {@link CustomKeyGenerator}. If null, {@link NonpartitionedKeyGenerator} will be used.
   * @param tableType the table type to use (MoR or CoW)
   * @return an instance of the class with this configuration
   */
  public static TestHudiTable withAdditionalColumns(
      String tableName,
      Path tempDir,
      JavaSparkContext jsc,
      String partitionConfig,
      HoodieTableType tableType,
      boolean tableUsesSparkClient) {
    return new TestHudiTable(
        tableName,
        addSchemaEvolutionFieldsToBase(BASIC_SCHEMA),
        tempDir,
        jsc,
        partitionConfig,
        tableType,
        tableUsesSparkClient);
  }

  public static TestHudiTable withAdditionalTopLevelField(
      String tableName,
      Path tempDir,
      JavaSparkContext jsc,
      String partitionConfig,
      HoodieTableType tableType,
      Schema previousSchema,
      boolean tableUsesSparkClient) {
    return new TestHudiTable(
        tableName,
        addTopLevelField(previousSchema),
        tempDir,
        jsc,
        partitionConfig,
        tableType,
        tableUsesSparkClient);
  }

  private TestHudiTable(
      String name,
      Schema schema,
      Path tempDir,
      JavaSparkContext jsc,
      String partitionConfig,
      HoodieTableType hoodieTableType,
      boolean tableUsesSparkClient) {
    try {
      this.tableName = name;
      this.schema = schema;
      // Initialize base path
      this.basePath = initBasePath(tempDir, name);
      // initialize spark session
      this.jsc = jsc;
      // Add key generator
      TypedProperties keyGenProperties = new TypedProperties();
      keyGenProperties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), RECORD_KEY_FIELD_NAME);
      if (partitionConfig == null) {
        this.keyGenerator = new NonpartitionedKeyGenerator(keyGenProperties);
        this.partitionFieldNames = Collections.emptyList();
      } else {
        keyGenProperties.put(PARTITIONPATH_FIELD_NAME.key(), partitionConfig);
        this.keyGenerator = new CustomKeyGenerator(keyGenProperties);
        this.partitionFieldNames =
            Arrays.stream(partitionConfig.split(","))
                .map(config -> config.split(":")[0])
                .collect(Collectors.toList());
      }
      // init Hoodie dataset and metaclient
      this.tableUsesSparkClient = tableUsesSparkClient;
      // init write clients
      if (tableUsesSparkClient) {
        this.sparkWriteClient = initSparkWriteClient(schema.toString(), keyGenProperties);
        this.metaClient = initMetaClient(jsc, hoodieTableType, keyGenProperties);
        this.javaWriteClient = null;
      } else {
        this.sparkWriteClient = null;
        this.metaClient = initMetaClient(hoodieTableType, keyGenProperties);
        this.javaWriteClient = initJavaWriteClient(schema.toString(), keyGenProperties);
      }
    } catch (IOException ex) {
      throw new UncheckedIOException("Unable to initialize TestHudiTable", ex);
    }
  }

  Schema getSchema() {
    return schema;
  }

  public HoodieActiveTimeline getActiveTimeline() {
    metaClient.reloadActiveTimeline();
    return metaClient.getActiveTimeline();
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
              value = BigDecimal.valueOf(RANDOM.nextLong(), 2);
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

  private HoodieRecord<HoodieAvroPayload> getRecord(
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

  public List<HoodieRecord<HoodieAvroPayload>> insertRecords(
      int numRecords, boolean checkForNoErrors) {
    List<HoodieRecord<HoodieAvroPayload>> inserts = generateRecords(numRecords);
    return insertRecords(checkForNoErrors, inserts);
  }

  public List<HoodieRecord<HoodieAvroPayload>> insertRecords(
      int numRecords, Object partitionValue, boolean checkForNoErrors) {
    Instant startTimeWindow = Instant.now().truncatedTo(ChronoUnit.DAYS).minus(1, ChronoUnit.DAYS);
    Instant endTimeWindow = Instant.now().truncatedTo(ChronoUnit.DAYS);
    List<HoodieRecord<HoodieAvroPayload>> inserts =
        IntStream.range(0, numRecords)
            .mapToObj(
                index ->
                    getRecord(
                        schema,
                        UUID.randomUUID().toString(),
                        startTimeWindow,
                        endTimeWindow,
                        null,
                        partitionValue))
            .collect(Collectors.toList());
    return insertRecords(checkForNoErrors, inserts);
  }

  public List<HoodieRecord<HoodieAvroPayload>> generateRecords(int numRecords) {
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
                        null))
            .collect(Collectors.toList());
    return inserts;
  }

  public String startCommit() {
    return getStartCommitInstant();
  }

  public List<HoodieRecord<HoodieAvroPayload>> insertRecordsWithCommitAlreadyStarted(
      List<HoodieRecord<HoodieAvroPayload>> inserts,
      String commitInstant,
      boolean checkForNoErrors) {
    List<WriteStatus> result;
    if (tableUsesSparkClient) {
      result = bulkInsertWithSparkClient(inserts, commitInstant);
    } else {
      result = bulkInsertWithJavaClient(inserts, commitInstant);
    }
    if (checkForNoErrors) {
      assertNoWriteErrors(result);
    }
    return inserts;
  }

  private List<HoodieRecord<HoodieAvroPayload>> insertRecords(
      boolean checkForNoErrors, List<HoodieRecord<HoodieAvroPayload>> inserts) {
    String instant = getStartCommitInstant();
    return insertRecordsWithCommitAlreadyStarted(inserts, instant, checkForNoErrors);
  }

  private List<HoodieRecord<HoodieAvroPayload>> generateUpdatesForRecords(
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

  public List<HoodieRecord<HoodieAvroPayload>> upsertRecordsWithCommitAlreadyStarted(
      List<HoodieRecord<HoodieAvroPayload>> records,
      String commitInstant,
      boolean checkForNoErrors) {
    List<HoodieRecord<HoodieAvroPayload>> updates = generateUpdatesForRecords(records);
    List<WriteStatus> result;
    if (tableUsesSparkClient) {
      result = upsertWithSparkClient(updates, commitInstant);
    } else {
      result = upsertWithJavaClient(updates, commitInstant);
    }
    if (checkForNoErrors) {
      assertNoWriteErrors(result);
    }
    return updates;
  }

  public List<HoodieRecord<HoodieAvroPayload>> upsertRecords(
      List<HoodieRecord<HoodieAvroPayload>> records, boolean checkForNoErrors) {
    String instant = getStartCommitInstant();
    return upsertRecordsWithCommitAlreadyStarted(records, instant, checkForNoErrors);
  }

  public List<HoodieKey> deleteRecords(
      List<HoodieRecord<HoodieAvroPayload>> records, boolean checkForNoErrors) {
    List<HoodieKey> deletes =
        records.stream().map(HoodieRecord::getKey).collect(Collectors.toList());
    String instant = getStartCommitInstant();
    List<WriteStatus> result;
    if (tableUsesSparkClient) {
      result = deleteWithSparkClient(deletes, instant);
    } else {
      result = deleteWithJavaClient(deletes, instant);
    }
    if (checkForNoErrors) {
      assertNoWriteErrors(result);
    }
    return deletes;
  }

  public void deletePartition(String partition, HoodieTableType tableType) {
    String actionType =
        CommitUtils.getCommitActionType(WriteOperationType.DELETE_PARTITION, tableType);
    String instant = getStartCommitOfActionType(actionType);
    List<WriteStatus> result;
    if (tableUsesSparkClient) {
      result = deletePartitionsWithSparkClient(Collections.singletonList(partition), instant);
    } else {
      result = deletePartitionsWithJavaClient(Collections.singletonList(partition), instant);
    }
    assertNoWriteErrors(result);
  }

  public void compact() {
    if (tableUsesSparkClient) {
      compactWithSparkClient();
    } else {
      compactWithJavaClient();
    }
  }

  public String onlyScheduleCompaction() {
    if (tableUsesSparkClient) {
      return scheduleCompactionWithSparkClient();
    }
    return scheduleCompactionWithJavaClient();
  }

  public void completeScheduledCompaction(String instant) {
    if (tableUsesSparkClient) {
      completeScheduledCompactionWithSparkClient(instant);
    } else {
      completeScheduledCompactionWithJavaClient(instant);
    }
  }

  public void cluster() {
    if (tableUsesSparkClient) {
      clusterWithSparkClient();
    } else {
      clusterWithJavaClient();
    }
  }

  public void rollback(String commitInstant) {
    if (tableUsesSparkClient) {
      rollbackWithSparkClient(commitInstant);
    } else {
      rollbackWithJavaClient(commitInstant);
    }
  }

  public void savepointRestoreForPreviousInstant() {
    List<HoodieInstant> commitInstants =
        metaClient.getActiveTimeline().reload().getCommitsTimeline().getInstants();
    HoodieInstant instantToRestore = commitInstants.get(commitInstants.size() - 2);
    if (tableUsesSparkClient) {
      savepointWithSparkClient(instantToRestore.getTimestamp(), "user", "savepoint-test");
      restoreToSavepointWithSparkClient(instantToRestore.getTimestamp());
    } else {
      savepointWithJavaClient(instantToRestore.getTimestamp(), "user", "savepoint-test");
      restoreToSavepointWithJavaClient(instantToRestore.getTimestamp());
    }
  }

  public void clean() {
    HoodieCleanMetadata metadata;
    if (tableUsesSparkClient) {
      metadata = cleanWithSparkClient();
    } else {
      metadata = cleanWithJavaClient();
    }
    // Assert that files are deleted to ensure test is realistic
    Assertions.assertTrue(metadata.getTotalFilesDeleted() > 0);
  }

  private String getStartCommitInstant() {
    if (tableUsesSparkClient) {
      return sparkWriteClient.startCommit(metaClient.getCommitActionType(), metaClient);
    }
    return javaWriteClient.startCommit(metaClient.getCommitActionType(), metaClient);
  }

  private String getStartCommitOfActionType(String actionType) {
    if (tableUsesSparkClient) {
      return sparkWriteClient.startCommit(actionType, metaClient);
    }
    return javaWriteClient.startCommit(actionType, metaClient);
  }

  public String getBasePath() {
    return basePath;
  }

  private SparkRDDWriteClient<HoodieAvroPayload> initSparkWriteClient(
      String schema, TypedProperties keyGenProperties) {
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
    HoodieArchivalConfig archivalConfig =
        HoodieArchivalConfig.newBuilder().archiveCommitsWith(3, 4).build();
    Properties lockProperties = new Properties();
    lockProperties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    lockProperties.setProperty(
        LockConfiguration.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "3000");
    lockProperties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY, "20");
    HoodieWriteConfig writeConfig =
        HoodieWriteConfig.newBuilder()
            .withProperties(keyGenProperties)
            .withPath(this.basePath)
            .withSchema(schema)
            .withKeyGenerator(keyGenerator.getClass().getCanonicalName())
            .withCompactionConfig(compactionConfig)
            .withClusteringConfig(clusteringConfig)
            .withCleanConfig(cleanConfig)
            .withArchivalConfig(archivalConfig)
            .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
            .withMarkersType(MarkerType.DIRECT.name())
            .withLockConfig(
                HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build())
            .withProperties(lockProperties)
            .build();
    HoodieEngineContext context = new HoodieSparkEngineContext(jsc);
    return new SparkRDDWriteClient<>(context, writeConfig);
  }

  private HoodieJavaWriteClient<HoodieAvroPayload> initJavaWriteClient(
      String schema, TypedProperties keyGenProperties) {
    // TODO(vamshigv): consolidate usage in java client code path.
    Configuration conf = new Configuration();
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
    HoodieArchivalConfig archivalConfig =
        HoodieArchivalConfig.newBuilder().archiveCommitsWith(3, 4).build();
    Properties lockProperties = new Properties();
    lockProperties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    lockProperties.setProperty(
        LockConfiguration.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "3000");
    lockProperties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY, "20");
    HoodieWriteConfig writeConfig =
        HoodieWriteConfig.newBuilder()
            .withProperties(keyGenProperties)
            .withPath(this.basePath)
            .withSchema(schema)
            .withKeyGenerator(keyGenerator.getClass().getCanonicalName())
            .withCompactionConfig(compactionConfig)
            .withClusteringConfig(clusteringConfig)
            .withCleanConfig(cleanConfig)
            .withArchivalConfig(archivalConfig)
            .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
            .withMarkersType(MarkerType.DIRECT.name())
            .withLockConfig(
                HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build())
            .withProperties(lockProperties)
            .build();
    HoodieEngineContext context = new HoodieJavaEngineContext(conf);
    return new HoodieJavaWriteClient<>(context, writeConfig);
  }

  private HoodieTableMetaClient initMetaClient(
      JavaSparkContext jsc, HoodieTableType hoodieTableType, TypedProperties keyGenProperties)
      throws IOException {
    LocalFileSystem fs = (LocalFileSystem) FSUtils.getFs(this.basePath, jsc.hadoopConfiguration());
    // Enforce checksun such that fs.open() is consistent to DFS
    fs.setVerifyChecksum(true);
    fs.mkdirs(new org.apache.hadoop.fs.Path(this.basePath));

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
    return HoodieTableMetaClient.initTableAndGetMetaClient(
        jsc.hadoopConfiguration(), this.basePath, properties);
  }

  private HoodieTableMetaClient initMetaClient(
      HoodieTableType hoodieTableType, TypedProperties keyGenProperties) throws IOException {
    Configuration conf = new Configuration();
    LocalFileSystem fs = (LocalFileSystem) FSUtils.getFs(this.basePath, conf);
    // Enforce checksun such that fs.open() is consistent to DFS
    fs.setVerifyChecksum(true);
    fs.mkdirs(new org.apache.hadoop.fs.Path(this.basePath));

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

  // Create the base path and store it for reference
  private String initBasePath(Path tempDir, String tableName) throws IOException {
    // make sure that table name in hudi is not coupled to path
    Path basePath = tempDir.resolve(tableName + "_v1");
    Files.createDirectories(basePath);
    return basePath.toUri().toString();
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

  @Override
  public void close() {
    if (sparkWriteClient != null) {
      sparkWriteClient.close();
    }
    if (javaWriteClient != null) {
      javaWriteClient.close();
    }
  }

  private List<WriteStatus> bulkInsertWithSparkClient(
      List<HoodieRecord<HoodieAvroPayload>> inserts, String commitInstant) {
    JavaRDD<HoodieRecord<HoodieAvroPayload>> writeRecords = jsc.parallelize(inserts, 1);
    JavaRDD<WriteStatus> result = sparkWriteClient.bulkInsert(writeRecords, commitInstant);
    return result.collect();
  }

  private List<WriteStatus> bulkInsertWithJavaClient(
      List<HoodieRecord<HoodieAvroPayload>> inserts, String commitInstant) {
    return javaWriteClient.bulkInsert(inserts, commitInstant);
  }

  private List<WriteStatus> upsertWithSparkClient(
      List<HoodieRecord<HoodieAvroPayload>> upserts, String commitInstant) {
    JavaRDD<HoodieRecord<HoodieAvroPayload>> writeRecords = jsc.parallelize(upserts, 1);
    JavaRDD<WriteStatus> result = sparkWriteClient.upsert(writeRecords, commitInstant);
    return result.collect();
  }

  private List<WriteStatus> upsertWithJavaClient(
      List<HoodieRecord<HoodieAvroPayload>> upserts, String commitInstant) {
    return javaWriteClient.upsert(upserts, commitInstant);
  }

  private List<WriteStatus> deleteWithSparkClient(List<HoodieKey> deletes, String commitInstant) {
    JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(deletes, 1);
    JavaRDD<WriteStatus> result = sparkWriteClient.delete(deleteKeys, commitInstant);
    return result.collect();
  }

  private List<WriteStatus> deleteWithJavaClient(List<HoodieKey> deletes, String commitInstant) {
    return javaWriteClient.delete(deletes, commitInstant);
  }

  private List<WriteStatus> deletePartitionsWithSparkClient(
      List<String> partitions, String commitInstant) {
    HoodieWriteResult writeResult = sparkWriteClient.deletePartitions(partitions, commitInstant);
    return writeResult.getWriteStatuses().collect();
  }

  private List<WriteStatus> deletePartitionsWithJavaClient(
      List<String> partitions, String commitInstant) {
    throw new UnsupportedOperationException(
        "Hoodie java client does not support delete partitions");
  }

  private String scheduleCompactionWithSparkClient() {
    return sparkWriteClient.scheduleCompaction(Option.empty()).get();
  }

  private String scheduleCompactionWithJavaClient() {
    return javaWriteClient.scheduleCompaction(Option.empty()).get();
  }

  private void completeScheduledCompactionWithSparkClient(String instant) {
    sparkWriteClient.compact(instant);
  }

  private void completeScheduledCompactionWithJavaClient(String instant) {
    javaWriteClient.compact(instant);
  }

  private void compactWithSparkClient() {
    String instant = sparkWriteClient.scheduleCompaction(Option.empty()).get();
    sparkWriteClient.compact(instant);
  }

  private void compactWithJavaClient() {
    String instant = javaWriteClient.scheduleCompaction(Option.empty()).get();
    javaWriteClient.compact(instant);
  }

  private void clusterWithSparkClient() {
    String instant = sparkWriteClient.scheduleClustering(Option.empty()).get();
    sparkWriteClient.cluster(instant, true);
  }

  private void clusterWithJavaClient() {
    String instant = javaWriteClient.scheduleClustering(Option.empty()).get();
    javaWriteClient.cluster(instant, true);
  }

  private void rollbackWithSparkClient(String instant) {
    sparkWriteClient.rollback(instant);
  }

  private void rollbackWithJavaClient(String instant) {
    javaWriteClient.rollback(instant);
  }

  private void savepointWithSparkClient(String instant, String user, String comment) {
    sparkWriteClient.savepoint(instant, user, comment);
  }

  private void savepointWithJavaClient(String instant, String user, String comment) {
    javaWriteClient.savepoint(instant, user, comment);
  }

  private void restoreToSavepointWithSparkClient(String instant) {
    sparkWriteClient.restoreToSavepoint(instant);
  }

  private void restoreToSavepointWithJavaClient(String instant) {
    javaWriteClient.restoreToSavepoint(instant);
  }

  private HoodieCleanMetadata cleanWithSparkClient() {
    return sparkWriteClient.clean();
  }

  private HoodieCleanMetadata cleanWithJavaClient() {
    return javaWriteClient.clean();
  }

  private static Schema addSchemaEvolutionFieldsToBase(Schema schema) {
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

  private static Schema addTopLevelField(Schema schema) {
    Schema nullableStringSchema =
        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));

    List<Schema.Field> newFields =
        new ArrayList<>(
            schema.getFields().stream().map(TestHudiTable::copyField).collect(Collectors.toList()));
    newFields.add(new Schema.Field("new_top_level_field", nullableStringSchema, "", null));
    return Schema.createRecord(
        schema.getName(), schema.getDoc(), schema.getNamespace(), false, newFields);
  }

  private static Schema.Field copyField(Schema.Field input) {
    return new Schema.Field(input.name(), input.schema(), input.doc(), input.defaultVal());
  }
}
