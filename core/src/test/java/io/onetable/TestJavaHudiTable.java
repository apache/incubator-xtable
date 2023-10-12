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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.CustomKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

public class TestJavaHudiTable extends TestAbstractHudiTable {
  private final HoodieJavaWriteClient<HoodieAvroPayload> javaWriteClient;
  private final Configuration conf;
  /**
   * Create a test table instance for general testing. The table is created with the schema defined
   * in basic_schema.avsc which contains many data types to ensure they are handled correctly.
   *
   * @param tableName name of the table used in the test, should be unique per test within a shared
   *     directory
   * @param tempDir directory where table will be written, typically a temporary directory that will
   *     be cleaned up after the tests.
   * @param partitionConfig sets the property `hoodie.datasource.write.partitionpath.field` for the
   *     {@link CustomKeyGenerator}. If null, {@link NonpartitionedKeyGenerator} will be used.
   * @param tableType the table type to use (MoR or CoW)
   * @return an instance of the class with this configuration
   */
  public static TestJavaHudiTable forStandardSchema(
      String tableName, Path tempDir, String partitionConfig, HoodieTableType tableType) {
    return new TestJavaHudiTable(tableName, BASIC_SCHEMA, tempDir, partitionConfig, tableType);
  }

  /**
   * Create a test table instance with a schema that has more fields than an instance returned by
   * {@link #forStandardSchema(String, Path, String, HoodieTableType)}. Specifically this instance
   * will add a top level field, nested field, field within a list, and field within a map to ensure
   * schema evolution is properly handled.
   *
   * @param tableName name of the table used in the test, should be unique per test within a shared
   *     directory
   * @param tempDir directory where table will be written, typically a temporary directory that will
   *     be cleaned up after the tests.
   * @param partitionConfig sets the property `hoodie.datasource.write.partitionpath.field` for the
   *     {@link CustomKeyGenerator}. If null, {@link NonpartitionedKeyGenerator} will be used.
   * @param tableType the table type to use (MoR or CoW)
   * @return an instance of the class with this configuration
   */
  public static TestJavaHudiTable withAdditionalColumns(
      String tableName, Path tempDir, String partitionConfig, HoodieTableType tableType) {
    return new TestJavaHudiTable(
        tableName,
        addSchemaEvolutionFieldsToBase(BASIC_SCHEMA),
        tempDir,
        partitionConfig,
        tableType);
  }

  public static TestJavaHudiTable withAdditionalTopLevelField(
      String tableName,
      Path tempDir,
      String partitionConfig,
      HoodieTableType tableType,
      Schema previousSchema) {
    return new TestJavaHudiTable(
        tableName, addTopLevelField(previousSchema), tempDir, partitionConfig, tableType);
  }

  private TestJavaHudiTable(
      String name,
      Schema schema,
      Path tempDir,
      String partitionConfig,
      HoodieTableType hoodieTableType) {
    try {
      this.tableName = name;
      this.schema = schema;
      // Initialize base path
      this.basePath = initBasePath(tempDir, name);
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
      this.conf = new Configuration();
      this.conf.set("parquet.avro.write-old-list-structure", "false");
      this.metaClient = initMetaClient(hoodieTableType, keyGenProperties);
      this.javaWriteClient = initJavaWriteClient(schema.toString(), keyGenProperties);
    } catch (IOException ex) {
      throw new UncheckedIOException("Unable to initialize TestJavaHudiTable", ex);
    }
  }

  public List<HoodieRecord<HoodieAvroPayload>> insertRecordsWithCommitAlreadyStarted(
      List<HoodieRecord<HoodieAvroPayload>> inserts,
      String commitInstant,
      boolean checkForNoErrors) {
    List<WriteStatus> result = javaWriteClient.bulkInsert(inserts, commitInstant);
    if (checkForNoErrors) {
      assertNoWriteErrors(result);
    }
    return inserts;
  }

  public List<HoodieRecord<HoodieAvroPayload>> upsertRecordsWithCommitAlreadyStarted(
      List<HoodieRecord<HoodieAvroPayload>> records,
      String commitInstant,
      boolean checkForNoErrors) {
    List<HoodieRecord<HoodieAvroPayload>> updates = generateUpdatesForRecords(records);
    List<WriteStatus> result = javaWriteClient.upsert(updates, commitInstant);
    if (checkForNoErrors) {
      assertNoWriteErrors(result);
    }
    return updates;
  }

  public List<HoodieKey> deleteRecords(
      List<HoodieRecord<HoodieAvroPayload>> records, boolean checkForNoErrors) {
    List<HoodieKey> deletes =
        records.stream().map(HoodieRecord::getKey).collect(Collectors.toList());
    String instant = getStartCommitInstant();
    List<WriteStatus> result = javaWriteClient.delete(deletes, instant);
    if (checkForNoErrors) {
      assertNoWriteErrors(result);
    }
    return deletes;
  }

  public void deletePartition(String partition, HoodieTableType tableType) {
    throw new UnsupportedOperationException(
        "Hoodie java client does not support delete partitions");
  }

  public void compact() {
    String instant = javaWriteClient.scheduleCompaction(Option.empty()).get();
    javaWriteClient.compact(instant);
  }

  public String onlyScheduleCompaction() {
    return javaWriteClient.scheduleCompaction(Option.empty()).get();
  }

  public void completeScheduledCompaction(String instant) {
    javaWriteClient.compact(instant);
  }

  public void cluster() {
    String instant = javaWriteClient.scheduleClustering(Option.empty()).get();
    javaWriteClient.cluster(instant, true);
  }

  public void rollback(String commitInstant) {
    javaWriteClient.rollback(commitInstant);
  }

  public void savepointRestoreForPreviousInstant() {
    List<HoodieInstant> commitInstants =
        metaClient.getActiveTimeline().reload().getCommitsTimeline().getInstants();
    HoodieInstant instantToRestore = commitInstants.get(commitInstants.size() - 2);
    javaWriteClient.savepoint(instantToRestore.getTimestamp(), "user", "savepoint-test");
    javaWriteClient.restoreToSavepoint(instantToRestore.getTimestamp());
  }

  public void clean() {
    HoodieCleanMetadata metadata = javaWriteClient.clean();
    // Assert that files are deleted to ensure test is realistic
    Assertions.assertTrue(metadata.getTotalFilesDeleted() > 0);
  }

  private String getStartCommitInstant() {
    return javaWriteClient.startCommit(metaClient.getCommitActionType(), metaClient);
  }

  private String getStartCommitOfActionType(String actionType) {
    return javaWriteClient.startCommit(actionType, metaClient);
  }

  public String startCommit() {
    return getStartCommitInstant();
  }

  public List<HoodieRecord<HoodieAvroPayload>> upsertRecords(
      List<HoodieRecord<HoodieAvroPayload>> records, boolean checkForNoErrors) {
    String instant = getStartCommitInstant();
    return upsertRecordsWithCommitAlreadyStarted(records, instant, checkForNoErrors);
  }

  private List<HoodieRecord<HoodieAvroPayload>> insertRecords(
      boolean checkForNoErrors, List<HoodieRecord<HoodieAvroPayload>> inserts) {
    String instant = getStartCommitInstant();
    return insertRecordsWithCommitAlreadyStarted(inserts, instant, checkForNoErrors);
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

  @Override
  public void close() {
    if (javaWriteClient != null) {
      javaWriteClient.close();
    }
  }

  private HoodieTableMetaClient initMetaClient(
      HoodieTableType hoodieTableType, TypedProperties keyGenProperties) throws IOException {
    return getMetaClient(keyGenProperties, hoodieTableType, conf);
  }

  private HoodieJavaWriteClient<HoodieAvroPayload> initJavaWriteClient(
      String schema, TypedProperties keyGenProperties) {
    HoodieWriteConfig writeConfig = generateWriteConfig(schema, keyGenProperties);
    HoodieEngineContext context = new HoodieJavaEngineContext(conf);
    return new HoodieJavaWriteClient<>(context, writeConfig);
  }
}
