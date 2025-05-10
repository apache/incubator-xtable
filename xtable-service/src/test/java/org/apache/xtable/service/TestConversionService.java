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
 
package org.apache.xtable.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.iceberg.SchemaParser;

import org.apache.xtable.avro.AvroSchemaConverter;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.iceberg.IcebergSchemaExtractor;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.schema.SparkSchemaExtractor;
import org.apache.xtable.service.models.ConvertTableRequest;
import org.apache.xtable.service.models.ConvertTableResponse;
import org.apache.xtable.service.models.ConvertedTable;
import org.apache.xtable.spi.extractor.ConversionSource;

@ExtendWith(MockitoExtension.class)
class TestConversionService {
  private static final String SOURCE_NAME = "users";
  private static final String SOURCE_PATH = "s3://bucket/tables/users";
  private static final String HUDI_META_PATH = "s3://bucket/tables/users/.hoodie";
  private static final String ICEBERG_META_PATH =
      "s3://bucket/tables/users/metadata/v1.metadata.json";
  private static final String DELTA_META_PATH = "s3://bucket/tables/users/delta_log";

  private static final String HUDI_SCHEMA_JSON =
      "{\n"
          + "  \"type\":\"record\",\n"
          + "  \"name\":\"Users\",\n"
          + "  \"fields\":[{\"name\":\"id\",\"type\":\"string\"}]\n"
          + "}";

  private static final String ICEBERG_JSON =
      "{\"type\":\"record\",\"name\":\"Users\","
          + "\"fields\":[{\"name\":\"id\",\"type\":\"string\",\"field-id\":1}]}";

  private static final String DELTA_JSON =
      "{\"type\":\"struct\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}";

  @Mock private ConversionServiceConfig serviceConfig;

  @Mock private ConversionController controller;

  @Mock ConversionSourceProvider provider;

  @Mock ConversionSource conversionSrc;

  @Mock InternalTable internalTbl;

  @Mock InternalSchema internalSchema;

  private ConversionService service;
  private Configuration conf;

  @BeforeEach
  void setUp() {
    this.conf = new Configuration();
    Map<String, ConversionSourceProvider<?>> providers = new HashMap<>();
    providers.put(TableFormat.DELTA, provider);
    providers.put(TableFormat.HUDI, provider);
    providers.put(TableFormat.ICEBERG, provider);
    service = new ConversionService(serviceConfig, controller, this.conf, providers);
  }

  @Test
  void convertToTargetHudi() {
    ConvertTableRequest req =
        ConvertTableRequest.builder()
            .sourceFormat(TableFormat.DELTA)
            .sourceTableName(SOURCE_NAME)
            .sourceTablePath(SOURCE_PATH)
            .targetFormats(Collections.singletonList(TableFormat.HUDI))
            .build();

    Schema avroSchema = new Schema.Parser().parse(HUDI_SCHEMA_JSON);
    try (MockedStatic<AvroSchemaConverter> avroConv = mockStatic(AvroSchemaConverter.class)) {
      when(controller.sync(any(), eq(provider))).thenReturn(null);
      when(provider.getConversionSourceInstance(any())).thenReturn(conversionSrc);
      when(conversionSrc.getCurrentTable()).thenReturn(internalTbl);

      when(internalTbl.getName()).thenReturn(TableFormat.HUDI);
      when(internalTbl.getLatestMetdataPath()).thenReturn(HUDI_META_PATH);
      when(internalTbl.getReadSchema()).thenReturn(internalSchema);

      AvroSchemaConverter converter = mock(AvroSchemaConverter.class);
      avroConv.when(AvroSchemaConverter::getInstance).thenReturn(converter);
      when(converter.fromInternalSchema(internalSchema)).thenReturn(avroSchema);

      ConvertTableResponse resp = service.convertTable(req);

      verify(controller).sync(any(), eq(provider));
      assertEquals(1, resp.getConvertedTables().size());
      ConvertedTable ct = resp.getConvertedTables().get(0);
      assertEquals(TableFormat.HUDI, ct.getTargetFormat());
      assertEquals(HUDI_META_PATH, ct.getTargetMetadataPath());
      assertEquals(avroSchema.toString(), ct.getTargetSchema());
    }
  }

  @Test
  void convertToTargetIceberg() {
    ConvertTableRequest req =
        ConvertTableRequest.builder()
            .sourceFormat(TableFormat.DELTA)
            .sourceTableName(SOURCE_NAME)
            .sourceTablePath(SOURCE_PATH)
            .targetFormats(Collections.singletonList(TableFormat.ICEBERG))
            .build();

    org.apache.iceberg.Schema icebergSchema = mock(org.apache.iceberg.Schema.class);
    try (MockedStatic<IcebergSchemaExtractor> iceExt = mockStatic(IcebergSchemaExtractor.class);
        MockedStatic<SchemaParser> parserMock = mockStatic(SchemaParser.class)) {

      when(controller.sync(any(), eq(provider))).thenReturn(null);
      when(provider.getConversionSourceInstance(any())).thenReturn(conversionSrc);
      when(conversionSrc.getCurrentTable()).thenReturn(internalTbl);

      when(internalTbl.getName()).thenReturn(TableFormat.ICEBERG);
      when(internalTbl.getLatestMetdataPath()).thenReturn(ICEBERG_META_PATH);
      when(internalTbl.getReadSchema()).thenReturn(internalSchema);

      IcebergSchemaExtractor extractor = mock(IcebergSchemaExtractor.class);
      iceExt.when(IcebergSchemaExtractor::getInstance).thenReturn(extractor);
      when(extractor.toIceberg(internalSchema)).thenReturn(icebergSchema);

      parserMock.when(() -> SchemaParser.toJson(icebergSchema)).thenReturn(ICEBERG_JSON);

      ConvertTableResponse resp = service.convertTable(req);

      verify(controller).sync(any(), eq(provider));
      assertEquals(1, resp.getConvertedTables().size());
      ConvertedTable ct = resp.getConvertedTables().get(0);
      assertEquals(TableFormat.ICEBERG, ct.getTargetFormat());
      assertEquals(ICEBERG_META_PATH, ct.getTargetMetadataPath());
      assertEquals(ICEBERG_JSON, ct.getTargetSchema());
    }
  }

  @Test
  void convertToTargetDelta() {
    ConvertTableRequest req =
        ConvertTableRequest.builder()
            .sourceFormat(TableFormat.ICEBERG)
            .sourceTableName(SOURCE_NAME)
            .sourceTablePath(SOURCE_PATH)
            .targetFormats(Collections.singletonList(TableFormat.DELTA))
            .build();

    StructType structType = mock(StructType.class);
    try (MockedStatic<SparkSchemaExtractor> sparkExt = mockStatic(SparkSchemaExtractor.class)) {
      when(controller.sync(any(), eq(provider))).thenReturn(null);
      when(provider.getConversionSourceInstance(any())).thenReturn(conversionSrc);
      when(conversionSrc.getCurrentTable()).thenReturn(internalTbl);

      when(internalTbl.getName()).thenReturn(TableFormat.DELTA);
      when(internalTbl.getLatestMetdataPath()).thenReturn(DELTA_META_PATH);
      when(internalTbl.getReadSchema()).thenReturn(internalSchema);

      SparkSchemaExtractor extractor = mock(SparkSchemaExtractor.class);
      sparkExt.when(SparkSchemaExtractor::getInstance).thenReturn(extractor);
      when(extractor.fromInternalSchema(internalSchema)).thenReturn(structType);
      when(structType.json()).thenReturn(DELTA_JSON);

      ConvertTableResponse resp = service.convertTable(req);

      verify(controller).sync(any(), eq(provider));
      assertEquals(1, resp.getConvertedTables().size());
      ConvertedTable ct = resp.getConvertedTables().get(0);
      assertEquals(TableFormat.DELTA, ct.getTargetFormat());
      assertEquals(DELTA_META_PATH, ct.getTargetMetadataPath());
      assertEquals(DELTA_JSON, ct.getTargetSchema());
    }
  }
}
