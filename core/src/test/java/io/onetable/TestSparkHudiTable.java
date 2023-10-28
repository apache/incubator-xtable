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

import static io.onetable.hudi.HudiTestUtil.getHoodieWriteConfig;
import static java.util.stream.Collectors.groupingBy;

import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Assertions;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.CustomKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.metadata.HoodieMetadataFileSystemView;

import com.google.common.base.Preconditions;

public class TestSparkHudiTable extends TestAbstractHudiTable
    implements GenericTable<HoodieRecord<HoodieAvroPayload>, String> {
  // typical inserts or upserts do not use this partition value.
  private static final String SPECIAL_PARTITION_VALUE = "GRANULAR";

  private final JavaSparkContext jsc;
  private SparkRDDWriteClient<HoodieAvroPayload> sparkWriteClient;

  public static TestSparkHudiTable forSchemaWithAdditionalColumnsAndPartitioning(
      String tableName, Path tempDir, JavaSparkContext jsc, boolean isPartitioned) {
    String partitionConfig = isPartitioned ? "level:SIMPLE" : null;
    return new TestSparkHudiTable(
        tableName,
        addSchemaEvolutionFieldsToBase(BASIC_SCHEMA),
        tempDir,
        jsc,
        partitionConfig,
        HoodieTableType.COPY_ON_WRITE);
  }

  /**
   * Create a test table instance for general testing. The table is created with the known schema
   * and standard partitioning if partitioning is enabled.
   *
   * @param tableName
   * @param tempDir
   * @param jsc
   * @param isPartitioned
   * @return
   */
  public static TestSparkHudiTable forStandardSchemaAndPartitioning(
      String tableName, Path tempDir, JavaSparkContext jsc, boolean isPartitioned) {
    String partitionConfig = isPartitioned ? "level:SIMPLE" : null;
    return new TestSparkHudiTable(
        tableName, BASIC_SCHEMA, tempDir, jsc, partitionConfig, HoodieTableType.COPY_ON_WRITE);
  }

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
  public static TestSparkHudiTable forStandardSchema(
      String tableName,
      Path tempDir,
      JavaSparkContext jsc,
      String partitionConfig,
      HoodieTableType tableType) {
    return new TestSparkHudiTable(
        tableName, BASIC_SCHEMA, tempDir, jsc, partitionConfig, tableType);
  }

  /**
   * Create a test table instance with a schema that has more fields than an instance returned by
   * {@link #forStandardSchema(String, Path, JavaSparkContext, String, HoodieTableType)}.
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
  public static TestSparkHudiTable withAdditionalColumns(
      String tableName,
      Path tempDir,
      JavaSparkContext jsc,
      String partitionConfig,
      HoodieTableType tableType) {
    return new TestSparkHudiTable(
        tableName,
        addSchemaEvolutionFieldsToBase(BASIC_SCHEMA),
        tempDir,
        jsc,
        partitionConfig,
        tableType);
  }

  private TestSparkHudiTable(
      String name,
      Schema schema,
      Path tempDir,
      JavaSparkContext jsc,
      String partitionConfig,
      HoodieTableType hoodieTableType) {
    super(name, schema, tempDir, partitionConfig);
    // initialize spark session
    this.jsc = jsc;
    this.sparkWriteClient = initSparkWriteClient(schema, typedProperties);
    this.metaClient = initMetaClient(jsc, hoodieTableType, typedProperties);
  }

  public List<HoodieRecord<HoodieAvroPayload>> insertRecordsWithCommitAlreadyStarted(
      List<HoodieRecord<HoodieAvroPayload>> inserts,
      String commitInstant,
      boolean checkForNoErrors) {
    JavaRDD<HoodieRecord<HoodieAvroPayload>> writeRecords = jsc.parallelize(inserts, 1);
    List<WriteStatus> result = sparkWriteClient.bulkInsert(writeRecords, commitInstant).collect();
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
    JavaRDD<HoodieRecord<HoodieAvroPayload>> writeRecords = jsc.parallelize(updates, 1);
    List<WriteStatus> result = sparkWriteClient.upsert(writeRecords, commitInstant).collect();
    if (checkForNoErrors) {
      assertNoWriteErrors(result);
    }
    return updates;
  }

  @Override
  public void deleteRows(List<HoodieRecord<HoodieAvroPayload>> records) {
    deleteRecords(records, true);
  }

  public List<HoodieKey> deleteRecords(
      List<HoodieRecord<HoodieAvroPayload>> records, boolean checkForNoErrors) {
    List<HoodieKey> deletes =
        records.stream().map(HoodieRecord::getKey).collect(Collectors.toList());
    JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(deletes, 1);
    String instant = getStartCommitInstant();
    List<WriteStatus> result = sparkWriteClient.delete(deleteKeys, instant).collect();
    if (checkForNoErrors) {
      assertNoWriteErrors(result);
    }
    return deletes;
  }

  public List<String> getAllLatestBaseFilePaths() {
    HoodieTableFileSystemView fsView =
        new HoodieMetadataFileSystemView(
            sparkWriteClient.getEngineContext(),
            metaClient,
            metaClient.reloadActiveTimeline(),
            getHoodieWriteConfig(metaClient).getMetadataConfig());
    return getAllLatestBaseFiles(fsView).stream()
        .map(HoodieBaseFile::getPath)
        .collect(Collectors.toList());
  }

  @Override
  public void deletePartition(String partition) {
    deletePartition(partition, HoodieTableType.COPY_ON_WRITE);
  }

  @Override
  public void deleteSpecialPartition() {
    deletePartition(SPECIAL_PARTITION_VALUE);
  }

  @Override
  public String getAnyPartitionValue(List<HoodieRecord<HoodieAvroPayload>> rows) {
    Map<String, List<HoodieRecord>> recordsByPartition =
        rows.stream().collect(groupingBy(HoodieRecord::getPartitionPath));
    return recordsByPartition.keySet().stream().sorted().findFirst().get();
  }

  public void deletePartition(String partition, HoodieTableType tableType) {
    Preconditions.checkArgument(
        partition == null || !partitionFieldNames.isEmpty(),
        "Table is not partitioned. Cannot delete partition.");
    String actionType =
        CommitUtils.getCommitActionType(WriteOperationType.DELETE_PARTITION, tableType);
    String instant = getStartCommitOfActionType(actionType);
    HoodieWriteResult writeResult =
        sparkWriteClient.deletePartitions(Collections.singletonList(partition), instant);
    List<WriteStatus> result = writeResult.getWriteStatuses().collect();
    assertNoWriteErrors(result);
  }

  @Override
  public String getOrderByColumn() {
    return "_hoodie_record_key";
  }

  public void compact() {
    String instant = sparkWriteClient.scheduleCompaction(Option.empty()).get();
    sparkWriteClient.compact(instant);
  }

  public String onlyScheduleCompaction() {
    return sparkWriteClient.scheduleCompaction(Option.empty()).get();
  }

  public void completeScheduledCompaction(String instant) {
    sparkWriteClient.compact(instant);
  }

  public void cluster() {
    String instant = sparkWriteClient.scheduleClustering(Option.empty()).get();
    sparkWriteClient.cluster(instant, true);
    // Reinitializing as clustering disables auto commit and we want to enable it back.
    sparkWriteClient = initSparkWriteClient(schema, typedProperties);
  }

  public void rollback(String commitInstant) {
    sparkWriteClient.rollback(commitInstant);
  }

  public void savepointRestoreForPreviousInstant() {
    List<HoodieInstant> commitInstants =
        metaClient.getActiveTimeline().reload().getCommitsTimeline().getInstants();
    HoodieInstant instantToRestore = commitInstants.get(commitInstants.size() - 2);
    sparkWriteClient.savepoint(instantToRestore.getTimestamp(), "user", "savepoint-test");
    sparkWriteClient.restoreToSavepoint(instantToRestore.getTimestamp());
  }

  public void clean() {
    HoodieCleanMetadata metadata = sparkWriteClient.clean();
    // Assert that files are deleted to ensure test is realistic
    Assertions.assertTrue(metadata.getTotalFilesDeleted() > 0);
  }

  private String getStartCommitInstant() {
    return sparkWriteClient.startCommit(metaClient.getCommitActionType(), metaClient);
  }

  private String getStartCommitOfActionType(String actionType) {
    return sparkWriteClient.startCommit(actionType, metaClient);
  }

  public String startCommit() {
    return getStartCommitInstant();
  }

  @Override
  public void upsertRows(List<HoodieRecord<HoodieAvroPayload>> records) {
    String instant = getStartCommitInstant();
    upsertRecordsWithCommitAlreadyStarted(records, instant, true);
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

  @Override
  public List<HoodieRecord<HoodieAvroPayload>> insertRows(int numRecords) {
    List<HoodieRecord<HoodieAvroPayload>> inserts = generateRecords(numRecords);
    return insertRecords(true, inserts);
  }

  public List<HoodieRecord<HoodieAvroPayload>> insertRecords(
      int numRecords, boolean checkForNoErrors) {
    List<HoodieRecord<HoodieAvroPayload>> inserts = generateRecords(numRecords);
    return insertRecords(checkForNoErrors, inserts);
  }

  @Override
  public List<HoodieRecord<HoodieAvroPayload>> insertRecordsForSpecialPartition(int numRecords) {
    return insertRecords(numRecords, SPECIAL_PARTITION_VALUE, true);
  }

  public List<HoodieRecord<HoodieAvroPayload>> insertRecords(
      int numRecords, Object partitionValue, boolean checkForNoErrors) {
    Preconditions.checkArgument(
        partitionValue == null || !partitionFieldNames.isEmpty(),
        "To insert records for a specific partition, table has to be partitioned.");
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
    if (sparkWriteClient != null) {
      sparkWriteClient.close();
    }
  }

  private SparkRDDWriteClient<HoodieAvroPayload> initSparkWriteClient(
      Schema schema, TypedProperties keyGenProperties) {
    HoodieWriteConfig writeConfig = generateWriteConfig(schema, keyGenProperties);
    HoodieEngineContext context = new HoodieSparkEngineContext(jsc);
    return new SparkRDDWriteClient<>(context, writeConfig);
  }

  private HoodieTableMetaClient initMetaClient(
      JavaSparkContext jsc, HoodieTableType hoodieTableType, TypedProperties keyGenProperties) {
    return getMetaClient(keyGenProperties, hoodieTableType, jsc.hadoopConfiguration());
  }
}
