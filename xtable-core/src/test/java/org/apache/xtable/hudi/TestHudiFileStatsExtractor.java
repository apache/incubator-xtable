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
 
package org.apache.xtable.hudi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.apache.xtable.GenericTable;
import org.apache.xtable.TestJavaHudiTable;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;

public class TestHudiFileStatsExtractor {
  private static final Schema AVRO_SCHEMA =
      new Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"Sample\",\"namespace\":\"test\",\"fields\":[{\"name\":\"long_field\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"key\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"nested_record\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Nested\",\"namespace\":\"test.nested_record\",\"fields\":[{\"name\":\"nested_int\",\"type\":\"int\",\"default\":0}]}],\"default\":null},{\"name\":\"repeated_record\",\"type\":{\"type\":\"array\",\"items\":\"test.nested_record.Nested\"},\"default\":[]},{\"name\":\"map_record\",\"type\":{\"type\":\"map\",\"values\":\"test.nested_record.Nested\"},\"default\":{}},{\"name\":\"date_field\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},{\"name\":\"timestamp_field\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"decimal_field\",\"type\":[\"null\",{\"type\":\"fixed\",\"name\":\"decimal_field\",\"size\":10,\"logicalType\":\"decimal\",\"precision\":20,\"scale\":2}],\"default\":null}]}");
  private static final Schema NESTED_SCHEMA =
      AVRO_SCHEMA.getField("nested_record").schema().getTypes().get(1);

  private final Configuration configuration = new Configuration();
  private final InternalField nestedIntBase = getNestedIntBase();
  private final InternalSchema nestedSchema = getNestedSchema(nestedIntBase, "nested_record");
  private final InternalField longField = getLongField();
  private final InternalField stringField = getStringField();
  private final InternalField dateField = getDateField();
  private final InternalField timestampField = getTimestampField();
  private final InternalField mapKeyField = getMapKeyField();
  private final InternalField mapValueField = getMapValueField(nestedIntBase);
  private final InternalField arrayField = getArrayField(nestedIntBase);
  private final InternalField decimalField = getDecimalField();

  private final InternalSchema schema =
      InternalSchema.builder()
          .name("schema")
          .fields(
              Arrays.asList(
                  longField,
                  stringField,
                  dateField,
                  timestampField,
                  InternalField.builder().name("nested_record").schema(nestedSchema).build(),
                  InternalField.builder()
                      .name("map_record")
                      .schema(
                          InternalSchema.builder()
                              .fields(Arrays.asList(mapKeyField, mapValueField))
                              .build())
                      .build(),
                  InternalField.builder()
                      .name("repeated_record")
                      .schema(
                          InternalSchema.builder()
                              .fields(Collections.singletonList(arrayField))
                              .build())
                      .build(),
                  decimalField))
          .build();

  @Test
  void columnStatsWithMetadataTable(@TempDir Path tempDir) throws Exception {
    String tableName = GenericTable.getTableName();
    String basePath;
    try (TestJavaHudiTable table =
        TestJavaHudiTable.withSchema(
            tableName, tempDir, "long_field:SIMPLE", HoodieTableType.COPY_ON_WRITE, AVRO_SCHEMA)) {
      List<HoodieRecord<HoodieAvroPayload>> records =
          getRecords().stream().map(this::buildRecord).collect(Collectors.toList());
      table.insertRecords(true, records);
      basePath = table.getBasePath();
    }
    HoodieTableMetadata tableMetadata =
        HoodieTableMetadata.create(
            new HoodieJavaEngineContext(configuration),
            HoodieMetadataConfig.newBuilder().enable(true).build(),
            basePath,
            true);
    Path parquetFile =
        Files.list(Paths.get(new URI(basePath)))
            .filter(path -> path.toString().endsWith(".parquet"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("No files found"));
    InternalDataFile inputFile =
        InternalDataFile.builder()
            .physicalPath(parquetFile.toString())
            .columnStats(Collections.emptyList())
            .fileFormat(FileFormat.APACHE_PARQUET)
            .lastModified(1234L)
            .fileSizeBytes(4321L)
            .recordCount(0)
            .build();
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(basePath).setConf(configuration).build();
    HudiFileStatsExtractor fileStatsExtractor = new HudiFileStatsExtractor(metaClient);
    List<InternalDataFile> output =
        fileStatsExtractor
            .addStatsToFiles(tableMetadata, Stream.of(inputFile), schema)
            .collect(Collectors.toList());
    validateOutput(output);
  }

  @Test
  void columnStatsWithoutMetadataTable(@TempDir Path tempDir) throws IOException {
    Path file = tempDir.resolve("tmp.parquet");
    GenericData genericData = GenericData.get();
    genericData.addLogicalTypeConversion(new Conversions.DecimalConversion());
    try (ParquetWriter<GenericRecord> writer =
        AvroParquetWriter.<GenericRecord>builder(
                HadoopOutputFile.fromPath(
                    new org.apache.hadoop.fs.Path(file.toUri()), configuration))
            .withSchema(AVRO_SCHEMA)
            .withDataModel(genericData)
            .build()) {
      for (GenericRecord record : getRecords()) {
        writer.write(record);
      }
    }

    InternalDataFile inputFile =
        InternalDataFile.builder()
            .physicalPath(file.toString())
            .columnStats(Collections.emptyList())
            .fileFormat(FileFormat.APACHE_PARQUET)
            .lastModified(1234L)
            .fileSizeBytes(4321L)
            .recordCount(0)
            .build();

    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockMetaClient.getHadoopConf()).thenReturn(configuration);
    HudiFileStatsExtractor fileStatsExtractor = new HudiFileStatsExtractor(mockMetaClient);
    List<InternalDataFile> output =
        fileStatsExtractor
            .addStatsToFiles(null, Stream.of(inputFile), schema)
            .collect(Collectors.toList());
    validateOutput(output);
  }

  private void validateOutput(List<InternalDataFile> output) {
    assertEquals(1, output.size());
    InternalDataFile fileWithStats = output.get(0);
    assertEquals(2, fileWithStats.getRecordCount());
    List<ColumnStat> columnStats = fileWithStats.getColumnStats();

    assertEquals(9, columnStats.size());

    ColumnStat longColumnStat =
        columnStats.stream().filter(stat -> stat.getField().equals(longField)).findFirst().get();
    assertEquals(1, longColumnStat.getNumNulls());
    assertEquals(2, longColumnStat.getNumValues());
    assertTrue(longColumnStat.getTotalSize() > 0);
    assertEquals(-25L, (Long) longColumnStat.getRange().getMinValue());
    assertEquals(-25L, (Long) longColumnStat.getRange().getMaxValue());

    ColumnStat stringColumnStat =
        columnStats.stream().filter(stat -> stat.getField().equals(stringField)).findFirst().get();
    assertEquals(0, stringColumnStat.getNumNulls());
    assertEquals(2, stringColumnStat.getNumValues());
    assertTrue(stringColumnStat.getTotalSize() > 0);
    assertEquals("another_example_string", stringColumnStat.getRange().getMinValue());
    assertEquals("example_string", stringColumnStat.getRange().getMaxValue());

    ColumnStat dateColumnStat =
        columnStats.stream().filter(stat -> stat.getField().equals(dateField)).findFirst().get();
    assertEquals(0, dateColumnStat.getNumNulls());
    assertEquals(2, dateColumnStat.getNumValues());
    assertTrue(dateColumnStat.getTotalSize() > 0);
    assertEquals(18181, dateColumnStat.getRange().getMinValue());
    assertEquals(18547, dateColumnStat.getRange().getMaxValue());

    ColumnStat timestampColumnStat =
        columnStats.stream()
            .filter(stat -> stat.getField().equals(timestampField))
            .findFirst()
            .get();
    assertEquals(0, timestampColumnStat.getNumNulls());
    assertEquals(2, timestampColumnStat.getNumValues());
    assertTrue(timestampColumnStat.getTotalSize() > 0);
    assertEquals(
        getInstant("2019-10-12").toEpochMilli(), timestampColumnStat.getRange().getMinValue());
    assertEquals(
        getInstant("2020-10-12").toEpochMilli(), timestampColumnStat.getRange().getMaxValue());

    ColumnStat nestedColumnStat =
        columnStats.stream()
            .filter(stat -> stat.getField().equals(nestedSchema.getFields().get(0)))
            .findFirst()
            .get();
    assertEquals(1, nestedColumnStat.getNumNulls());
    assertEquals(2, nestedColumnStat.getNumValues());
    assertEquals(2, nestedColumnStat.getRange().getMinValue());
    assertEquals(2, nestedColumnStat.getRange().getMaxValue());

    ColumnStat mapKeyColumnStat =
        columnStats.stream().filter(stat -> stat.getField().equals(mapKeyField)).findFirst().get();
    assertEquals(1, mapKeyColumnStat.getNumNulls());
    assertEquals(3, mapKeyColumnStat.getNumValues());
    assertEquals("key1", mapKeyColumnStat.getRange().getMinValue());
    assertEquals("key2", mapKeyColumnStat.getRange().getMaxValue());

    ColumnStat mapValueColumnStat =
        columnStats.stream()
            .filter(stat -> stat.getField().equals(mapValueField.getSchema().getFields().get(0)))
            .findFirst()
            .get();
    assertEquals(1, mapValueColumnStat.getNumNulls());
    assertEquals(3, mapValueColumnStat.getNumValues());
    assertEquals(13, mapValueColumnStat.getRange().getMinValue());
    assertEquals(23, mapValueColumnStat.getRange().getMaxValue());

    ColumnStat arrayElementColumnStat =
        columnStats.stream()
            .filter(stat -> stat.getField().equals(arrayField.getSchema().getFields().get(0)))
            .findFirst()
            .get();
    assertEquals(0, arrayElementColumnStat.getNumNulls());
    assertEquals(6, arrayElementColumnStat.getNumValues());
    assertEquals(1, arrayElementColumnStat.getRange().getMinValue());
    assertEquals(6, arrayElementColumnStat.getRange().getMaxValue());

    ColumnStat decimalColumnStat =
        columnStats.stream().filter(stat -> stat.getField().equals(decimalField)).findFirst().get();
    assertEquals(1, decimalColumnStat.getNumNulls());
    assertEquals(2, decimalColumnStat.getNumValues());
    assertTrue(decimalColumnStat.getTotalSize() > 0);
    assertEquals(
        new BigDecimal("1234.56"),
        ((BigDecimal) decimalColumnStat.getRange().getMinValue()).setScale(2));
    assertEquals(
        new BigDecimal("1234.56"),
        ((BigDecimal) decimalColumnStat.getRange().getMaxValue()).setScale(2));
  }

  private HoodieRecord<HoodieAvroPayload> buildRecord(GenericRecord record) {
    HoodieKey hoodieKey = new HoodieKey(record.get("key").toString(), "");
    return new HoodieAvroRecord<>(hoodieKey, new HoodieAvroPayload(Option.of(record)));
  }

  private List<GenericRecord> getRecords() {
    GenericRecord record1 =
        createRecord(
            -25L,
            "another_example_string",
            null,
            Arrays.asList(1, 2, 3),
            Collections.emptyMap(),
            getDate("2019-10-12"),
            getInstant("2019-10-12"),
            null);
    Map<String, Integer> map = new HashMap<>();
    map.put("key1", 13);
    map.put("key2", 23);
    GenericRecord record2 =
        createRecord(
            null,
            "example_string",
            2,
            Arrays.asList(4, 5, 6),
            map,
            getDate("2020-10-12"),
            getInstant("2020-10-12"),
            new BigDecimal("1234.56"));
    return Arrays.asList(record1, record2);
  }

  private InternalField getDecimalField() {
    Map<InternalSchema.MetadataKey, Object> metadata = new HashMap<>();
    metadata.put(InternalSchema.MetadataKey.DECIMAL_PRECISION, 20);
    metadata.put(InternalSchema.MetadataKey.DECIMAL_SCALE, 2);
    return InternalField.builder()
        .name("decimal_field")
        .schema(
            InternalSchema.builder()
                .name("decimal")
                .dataType(InternalType.DECIMAL)
                .metadata(metadata)
                .build())
        .build();
  }

  private InternalField getArrayField(InternalField nestedIntBase) {
    return InternalField.builder()
        .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
        .parentPath("repeated_record")
        .schema(
            getNestedSchema(
                nestedIntBase,
                "repeated_record." + InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME))
        .build();
  }

  private InternalField getMapValueField(InternalField nestedIntBase) {
    return InternalField.builder()
        .name(InternalField.Constants.MAP_VALUE_FIELD_NAME)
        .parentPath("map_record")
        .schema(
            getNestedSchema(
                nestedIntBase, "map_record." + InternalField.Constants.MAP_VALUE_FIELD_NAME))
        .build();
  }

  private InternalField getMapKeyField() {
    return InternalField.builder()
        .name(InternalField.Constants.MAP_KEY_FIELD_NAME)
        .parentPath("map_record")
        .schema(InternalSchema.builder().name("map_key").dataType(InternalType.STRING).build())
        .build();
  }

  private InternalField getTimestampField() {
    return InternalField.builder()
        .name("timestamp_field")
        .schema(InternalSchema.builder().name("time").dataType(InternalType.TIMESTAMP_NTZ).build())
        .build();
  }

  private InternalField getDateField() {
    return InternalField.builder()
        .name("date_field")
        .schema(InternalSchema.builder().name("date").dataType(InternalType.DATE).build())
        .build();
  }

  private InternalField getStringField() {
    return InternalField.builder()
        .name("key")
        .schema(InternalSchema.builder().name("string").dataType(InternalType.STRING).build())
        .build();
  }

  private InternalField getLongField() {
    return InternalField.builder()
        .name("long_field")
        .schema(InternalSchema.builder().name("long").dataType(InternalType.LONG).build())
        .build();
  }

  private InternalField getNestedIntBase() {
    return InternalField.builder()
        .name("nested_int")
        .schema(
            InternalSchema.builder()
                .name("int")
                .dataType(InternalType.INT)
                .isNullable(false)
                .build())
        .build();
  }

  private InternalSchema getNestedSchema(InternalField nestedIntBase, String parentPath) {
    return InternalSchema.builder()
        .name("nested")
        .dataType(InternalType.RECORD)
        .fields(Collections.singletonList(nestedIntBase.toBuilder().parentPath(parentPath).build()))
        .build();
  }

  private GenericRecord createRecord(
      Long longValue,
      String stringValue,
      Integer nestedIntValue,
      List<Integer> listValues,
      Map<String, Integer> mapValues,
      Date dateValue,
      Instant timestampValue,
      BigDecimal decimal) {
    GenericData.Record record = new GenericData.Record(AVRO_SCHEMA);
    record.put("long_field", longValue);
    record.put("key", stringValue);
    record.put("timestamp_field", timestampValue.toEpochMilli());
    record.put("date_field", dateValue.toLocalDate().toEpochDay());
    record.put("decimal_field", decimal);
    if (nestedIntValue != null) {
      GenericData.Record nested = getNestedRecord(nestedIntValue);
      record.put("nested_record", nested);
    }
    if (listValues != null) {
      List<GenericData.Record> recordList =
          listValues.stream().map(this::getNestedRecord).collect(Collectors.toList());
      record.put("repeated_record", recordList);
    }
    if (mapValues != null) {
      Map<String, GenericData.Record> recordMap =
          mapValues.entrySet().stream()
              .collect(
                  Collectors.toMap(Map.Entry::getKey, entry -> getNestedRecord(entry.getValue())));
      record.put("map_record", recordMap);
    }
    return record;
  }

  @NotNull
  private GenericData.Record getNestedRecord(Integer nestedIntValue) {
    GenericData.Record nested = new GenericData.Record(NESTED_SCHEMA);
    nested.put("nested_int", nestedIntValue);
    return nested;
  }

  private Date getDate(String dateStr) {
    return Date.valueOf(dateStr);
  }

  private Instant getInstant(String dateValue) {
    LocalDate localDate = LocalDate.parse(dateValue);
    return localDate.atStartOfDay().toInstant(ZoneOffset.UTC);
  }
}
