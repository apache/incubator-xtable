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
 
package org.apache.xtable.hudi.extensions;

import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_TABLE_NAME_KEY;
import static org.apache.hudi.common.table.HoodieTableConfig.VERSION;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.RECORDKEY_FIELD_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.BaseHoodieClient;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import org.apache.xtable.hudi.idtracking.IdTracker;

public class TestAddFieldIdsClientInitCallback {
  @TempDir static Path tempDir;

  private final IdTracker mockIdTracker = mock(IdTracker.class);
  private final AddFieldIdsClientInitCallback callback =
      new AddFieldIdsClientInitCallback(mockIdTracker);

  @Test
  void nullSchemasIsNoOp() {
    BaseHoodieClient client = mock(BaseHoodieClient.class);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(getTableBasePath()).build();
    when(client.getConfig()).thenReturn(config);
    callback.call(client);
    verifyNoInteractions(mockIdTracker);
  }

  @Test
  void noExistingTable() {
    Schema inputSchema = getSchemaStub(1);
    Schema updatedSchema = getSchemaStub(3);

    HoodieEngineContext localEngineContext = new HoodieLocalEngineContext(new Configuration());
    HoodieWriteConfig config =
        HoodieWriteConfig.newBuilder()
            .withSchema(inputSchema.toString())
            .withPopulateMetaFields(true)
            .withPath(getTableBasePath())
            .build();

    BaseHoodieClient client = setBaseHoodieClientMocks(localEngineContext, config);

    when(mockIdTracker.addIdTracking(inputSchema, Option.empty(), true)).thenReturn(updatedSchema);

    callback.call(client);

    assertEquals(updatedSchema.toString(), config.getSchema());
    assertEquals(updatedSchema.toString(), config.getWriteSchema());
  }

  @Test
  void existingTable() throws IOException {
    Schema existingSchema = getSchemaStub(1);
    Schema inputSchema = getSchemaStub(2);
    Schema updatedSchema = getSchemaStub(3);

    HoodieEngineContext localEngineContext = new HoodieJavaEngineContext(new Configuration());
    String basePath = getTableBasePath();
    HoodieWriteConfig tableConfig =
        HoodieWriteConfig.newBuilder()
            .withSchema(existingSchema.toString())
            .withPopulateMetaFields(true)
            .withIndexConfig(
                HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
            .withPath(basePath)
            .build();

    // create a commit to create an initial schema
    try (HoodieJavaWriteClient<HoodieAvroPayload> hoodieJavaWriteClient =
        new HoodieJavaWriteClient<>(localEngineContext, tableConfig)) {
      Properties properties = new Properties();
      properties.setProperty(HOODIE_TABLE_NAME_KEY, "test_table");
      properties.setProperty(PARTITIONPATH_FIELD_NAME.key(), "");
      properties.setProperty(RECORDKEY_FIELD_NAME.key(), "id");
      properties.setProperty(
          VERSION.key(), Integer.toString(HoodieTableVersion.current().versionCode()));
      HoodieTableMetaClient.initTableAndGetMetaClient(
          localEngineContext.getHadoopConf().get(), basePath, properties);
      String commit = hoodieJavaWriteClient.startCommit();
      GenericRecord genericRecord =
          new GenericRecordBuilder(existingSchema).set("id", "1").set("field", "value").build();
      HoodieRecord<HoodieAvroPayload> record =
          new HoodieAvroRecord<>(
              new HoodieKey("1", ""), new HoodieAvroPayload(Option.of(genericRecord)));
      hoodieJavaWriteClient.insert(Collections.singletonList(record), commit);
    }

    HoodieWriteConfig config =
        HoodieWriteConfig.newBuilder()
            .withSchema(inputSchema.toString())
            .withPopulateMetaFields(false)
            .withPath(basePath)
            .build();

    BaseHoodieClient client = setBaseHoodieClientMocks(localEngineContext, config);

    when(mockIdTracker.addIdTracking(
            inputSchema, Option.of(HoodieAvroUtils.addMetadataFields(existingSchema)), false))
        .thenReturn(updatedSchema);

    callback.call(client);

    assertEquals(updatedSchema.toString(), config.getSchema());
    assertEquals(updatedSchema.toString(), config.getWriteSchema());
  }

  @Test
  void writeSchemaOverrideProvided() {
    Schema inputSchema = getSchemaStub(1);
    Schema inputWriteSchema = getSchemaStub(2);
    Schema updatedSchema = getSchemaStub(3);
    Schema updatedWriteSchema = getSchemaStub(4);

    Properties properties = new Properties();
    properties.setProperty(
        HoodieWriteConfig.WRITE_SCHEMA_OVERRIDE.key(), inputWriteSchema.toString());

    HoodieEngineContext localEngineContext = new HoodieLocalEngineContext(new Configuration());
    HoodieWriteConfig config =
        HoodieWriteConfig.newBuilder()
            .withSchema(inputSchema.toString())
            .withProperties(properties)
            .withPopulateMetaFields(true)
            .withPath(getTableBasePath())
            .build();

    BaseHoodieClient client = setBaseHoodieClientMocks(localEngineContext, config);

    when(mockIdTracker.addIdTracking(inputSchema, Option.empty(), true)).thenReturn(updatedSchema);
    when(mockIdTracker.addIdTracking(inputWriteSchema, Option.empty(), true))
        .thenReturn(updatedWriteSchema);

    callback.call(client);

    assertEquals(updatedSchema.toString(), config.getSchema());
    assertEquals(updatedWriteSchema.toString(), config.getWriteSchema());
  }

  private static BaseHoodieClient setBaseHoodieClientMocks(
      HoodieEngineContext localEngineContext, HoodieWriteConfig config) {
    BaseHoodieClient client = mock(BaseHoodieClient.class);
    when(client.getConfig()).thenReturn(config);
    when(client.getEngineContext()).thenReturn(localEngineContext);
    return client;
  }

  private Schema getSchemaStub(int id) {
    return Schema.createRecord(
        "testing" + id,
        null,
        null,
        false,
        Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("field", Schema.create(Schema.Type.STRING), null, null)));
  }

  private String getTableBasePath() {
    return tempDir.toString() + "/" + UUID.randomUUID();
  }
}
