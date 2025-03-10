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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import lombok.AccessLevel;
import lombok.Getter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.plan.strategy.JavaSizeBasedClusteringPlanStrategy;
import org.apache.hudi.client.clustering.run.strategy.JavaSortAndSizeExecutionStrategy;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.CustomKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;

import com.google.common.base.Preconditions;

public class TestJavaHudiTable extends TestAbstractHudiTable {

  @Getter(value = AccessLevel.PROTECTED)
  private HoodieJavaWriteClient<HoodieAvroPayload> writeClient;

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
    return new TestJavaHudiTable(
        tableName, BASIC_SCHEMA, tempDir, partitionConfig, tableType, null);
  }

  public static TestJavaHudiTable forStandardSchema(
      String tableName,
      Path tempDir,
      String partitionConfig,
      HoodieTableType tableType,
      HoodieArchivalConfig archivalConfig) {
    return new TestJavaHudiTable(
        tableName, BASIC_SCHEMA, tempDir, partitionConfig, tableType, archivalConfig);
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
        tableType,
        null);
  }

  public static TestJavaHudiTable withAdditionalTopLevelField(
      String tableName,
      Path tempDir,
      String partitionConfig,
      HoodieTableType tableType,
      Schema previousSchema) {
    return new TestJavaHudiTable(
        tableName, addTopLevelField(previousSchema), tempDir, partitionConfig, tableType, null);
  }

  public static TestJavaHudiTable withSchema(
      String tableName,
      Path tempDir,
      String partitionConfig,
      HoodieTableType tableType,
      Schema schema) {
    return new TestJavaHudiTable(tableName, schema, tempDir, partitionConfig, tableType, null);
  }

  private TestJavaHudiTable(
      String name,
      Schema schema,
      Path tempDir,
      String partitionConfig,
      HoodieTableType hoodieTableType,
      HoodieArchivalConfig archivalConfig) {
    super(name, schema, tempDir, partitionConfig);
    this.conf = new Configuration();
    this.conf.set("parquet.avro.write-old-list-structure", "false");
    try {
      this.metaClient = initMetaClient(hoodieTableType, typedProperties);
    } catch (IOException ex) {
      throw new UncheckedIOException("Unable to initialize metaclient for TestJavaHudiTable", ex);
    }
    this.writeClient = initJavaWriteClient(schema, typedProperties, archivalConfig);
  }

  public List<HoodieRecord<HoodieAvroPayload>> insertRecordsWithCommitAlreadyStarted(
      List<HoodieRecord<HoodieAvroPayload>> inserts,
      String commitInstant,
      boolean checkForNoErrors) {
    List<WriteStatus> result = writeClient.bulkInsert(copyRecords(inserts), commitInstant);
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
    List<WriteStatus> result = writeClient.upsert(copyRecords(updates), commitInstant);
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
    List<WriteStatus> result = writeClient.delete(deletes, instant);
    if (checkForNoErrors) {
      assertNoWriteErrors(result);
    }
    return deletes;
  }

  public void deletePartition(String partition, HoodieTableType tableType) {
    throw new UnsupportedOperationException(
        "Hoodie java client does not support delete partitions");
  }

  public void cluster() {
    String instant = writeClient.scheduleClustering(Option.empty()).get();
    writeClient.cluster(instant, true);
    // Reinitializing as clustering disables auto commit and we want to enable it back.
    writeClient = initJavaWriteClient(schema, typedProperties, null);
  }

  public List<HoodieRecord<HoodieAvroPayload>> insertRecords(
      int numRecords, boolean checkForNoErrors) {
    List<HoodieRecord<HoodieAvroPayload>> inserts = generateRecords(numRecords);
    return insertRecords(checkForNoErrors, inserts);
  }

  public List<HoodieRecord<HoodieAvroPayload>> insertRecords(
      int numRecords, Object partitionValue, boolean checkForNoErrors) {
    Preconditions.checkArgument(
        partitionValue == null || !partitionFieldNames.isEmpty(),
        "To insert records for a specific partition, table has to be partitioned.");
    Instant startTimeWindow = Instant.now().truncatedTo(ChronoUnit.DAYS).minus(1, ChronoUnit.DAYS);
    Instant endTimeWindow = Instant.now().truncatedTo(ChronoUnit.DAYS);
    List<HoodieRecord<HoodieAvroPayload>> inserts =
        getHoodieRecordStream(numRecords, partitionValue, startTimeWindow, endTimeWindow)
            .collect(Collectors.toList());
    return insertRecords(checkForNoErrors, inserts);
  }

  public List<HoodieRecord<HoodieAvroPayload>> insertRecords(
      int numRecords, List<Object> partitionValues, boolean checkForNoErrors) {
    Preconditions.checkArgument(
        partitionValues.isEmpty() || !partitionFieldNames.isEmpty(),
        "To insert records for a specific partitions, table has to be partitioned.");
    Instant startTimeWindow = Instant.now().truncatedTo(ChronoUnit.DAYS).minus(1, ChronoUnit.DAYS);
    Instant endTimeWindow = Instant.now().truncatedTo(ChronoUnit.DAYS);
    List<HoodieRecord<HoodieAvroPayload>> inserts =
        partitionValues.stream()
            .flatMap(
                partitionValue ->
                    getHoodieRecordStream(
                        numRecords, partitionValue, startTimeWindow, endTimeWindow))
            .collect(Collectors.toList());
    return insertRecords(checkForNoErrors, inserts);
  }

  private Stream<HoodieRecord<HoodieAvroPayload>> getHoodieRecordStream(
      int numRecords, Object partitionValue, Instant startTimeWindow, Instant endTimeWindow) {
    return IntStream.range(0, numRecords)
        .mapToObj(
            index ->
                getRecord(
                    schema,
                    UUID.randomUUID().toString(),
                    startTimeWindow,
                    endTimeWindow,
                    null,
                    partitionValue));
  }

  @Override
  public void deletePartition(String partitionValue) {
    throw new UnsupportedOperationException(
        "Hoodie java client does not support delete partitions");
  }

  @Override
  public void deleteSpecialPartition() {
    throw new UnsupportedOperationException(
        "Hoodie java client does not support delete partitions");
  }

  @Override
  public void close() {
    if (writeClient != null) {
      writeClient.close();
    }
  }

  // Existing records are need to create updates etc... so create a copy of the records and operate
  // on the copy.
  private List<HoodieRecord<HoodieAvroPayload>> copyRecords(
      List<HoodieRecord<HoodieAvroPayload>> records) {
    return records.stream()
        .map(
            hoodieRecord -> {
              HoodieKey key = hoodieRecord.getKey();
              byte[] payloadBytes = hoodieRecord.getData().getRecordBytes();
              byte[] payloadBytesCopy = Arrays.copyOf(payloadBytes, payloadBytes.length);
              try {
                GenericRecord recordCopy = HoodieAvroUtils.bytesToAvro(payloadBytesCopy, schema);
                return new HoodieAvroRecord<>(key, new HoodieAvroPayload(Option.of(recordCopy)));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .collect(Collectors.toList());
  }

  private HoodieTableMetaClient initMetaClient(
      HoodieTableType hoodieTableType, TypedProperties keyGenProperties) throws IOException {
    return getMetaClient(keyGenProperties, hoodieTableType, conf);
  }

  private HoodieJavaWriteClient<HoodieAvroPayload> initJavaWriteClient(
      Schema schema, TypedProperties keyGenProperties, HoodieArchivalConfig archivalConfig) {
    HoodieWriteConfig writeConfig =
        HoodieWriteConfig.newBuilder()
            .withProperties(generateWriteConfig(schema, keyGenProperties).getProps())
            .withClusteringConfig(
                HoodieClusteringConfig.newBuilder()
                    .withClusteringPlanStrategyClass(
                        JavaSizeBasedClusteringPlanStrategy.class.getName())
                    .withClusteringExecutionStrategyClass(
                        JavaSortAndSizeExecutionStrategy.class.getName())
                    .build())
            .build();
    // override archival config if provided
    if (archivalConfig != null) {
      writeConfig =
          HoodieWriteConfig.newBuilder()
              .withProperties(writeConfig.getProps())
              .withArchivalConfig(archivalConfig)
              .build();
    }
    HoodieEngineContext context = new HoodieJavaEngineContext(conf);
    return new HoodieJavaWriteClient<>(context, writeConfig);
  }
}
