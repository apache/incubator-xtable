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
 
package io.onetable.hudi;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
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

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.stat.ColumnStat;

public class TestHudiFileStatsExtractor {
  private static final Schema AVRO_SCHEMA =
      new Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"Sample\",\"namespace\":\"test\",\"fields\":[{\"name\":\"long_field\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"string_field\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"nested_record\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Nested\",\"namespace\":\"test.nested_record\",\"fields\":[{\"name\":\"nested_int\",\"type\":\"int\",\"default\":0}]}],\"default\":null},{\"name\":\"repeated_record\",\"type\":{\"type\":\"array\",\"items\":\"test.nested_record.Nested\"},\"default\":[]},{\"name\":\"map_record\",\"type\":{\"type\":\"map\",\"values\":\"test.nested_record.Nested\"},\"default\":{}},{\"name\":\"date_field\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},{\"name\":\"timestamp_field\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null}]}");
  private static final Schema NESTED_SCHEMA =
      AVRO_SCHEMA.getField("nested_record").schema().getTypes().get(1);

  @Test
  public void columnStatsTest(@TempDir Path tempDir) throws IOException {
    Configuration configuration = new Configuration();
    Path file = tempDir.resolve("tmp.parquet");
    try (ParquetWriter<GenericRecord> writer =
        AvroParquetWriter.<GenericRecord>builder(
                HadoopOutputFile.fromPath(
                    new org.apache.hadoop.fs.Path(file.toUri()), configuration))
            .withSchema(AVRO_SCHEMA)
            .build()) {

      GenericRecord record1 =
          createRecord(
              -25L,
              "another_example_string",
              null,
              Arrays.asList(1, 2, 3),
              Collections.emptyMap(),
              getDate("2019-10-12"),
              getInstant("2019-10-12"));
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
              getInstant("2020-10-12"));
      writer.write(record1);
      writer.write(record2);
    }

    OneField nestedIntBase = getNestedIntBase();
    OneSchema nestedSchema = getNestedSchema(nestedIntBase, "nested_record");
    OneField longField = getLongField();
    OneField stringField = getStringField();
    OneField dateField = getDateField();
    OneField timestampField = getTimestampField();
    OneField mapKeyField = getMapKeyField();
    OneField mapValueField = getMapValueField(nestedIntBase);
    OneField arrayField = getArrayField(nestedIntBase);

    OneSchema schema =
        OneSchema.builder()
            .name("schema")
            .fields(
                Arrays.asList(
                    longField,
                    stringField,
                    dateField,
                    timestampField,
                    OneField.builder().name("nested_record").schema(nestedSchema).build(),
                    OneField.builder()
                        .name("map_record")
                        .schema(
                            OneSchema.builder()
                                .fields(Arrays.asList(mapKeyField, mapValueField))
                                .build())
                        .build(),
                    OneField.builder()
                        .name("repeated_record")
                        .schema(
                            OneSchema.builder()
                                .fields(Collections.singletonList(arrayField))
                                .build())
                        .build()))
            .build();

    HudiFileStats fileStats =
        HudiFileStatsExtractor.getInstance()
            .computeColumnStatsForFile(
                new org.apache.hadoop.fs.Path(file.toString()), configuration, schema);
    assertEquals(2, fileStats.getRowCount());
    Map<OneField, ColumnStat> columnStats = fileStats.getColumnStats();

    assertEquals(8, columnStats.size());

    ColumnStat longColumnStat = columnStats.get(longField);
    assertEquals(1, longColumnStat.getNumNulls());
    assertEquals(2, longColumnStat.getNumValues());
    assertEquals(37, longColumnStat.getTotalSize());
    assertEquals(-25L, (Long) longColumnStat.getRange().getMinValue());
    assertEquals(-25L, (Long) longColumnStat.getRange().getMaxValue());

    ColumnStat stringColumnStat = columnStats.get(stringField);
    assertEquals(0, stringColumnStat.getNumNulls());
    assertEquals(2, stringColumnStat.getNumValues());
    assertEquals(67, stringColumnStat.getTotalSize());
    assertEquals("another_example_string", stringColumnStat.getRange().getMinValue());
    assertEquals("example_string", stringColumnStat.getRange().getMaxValue());

    ColumnStat dateColumnStat = columnStats.get(dateField);
    assertEquals(0, dateColumnStat.getNumNulls());
    assertEquals(2, dateColumnStat.getNumValues());
    assertEquals(31, dateColumnStat.getTotalSize());
    assertEquals(18181, dateColumnStat.getRange().getMinValue());
    assertEquals(18547, dateColumnStat.getRange().getMaxValue());

    ColumnStat timestampColumnStat = columnStats.get(timestampField);
    assertEquals(0, timestampColumnStat.getNumNulls());
    assertEquals(2, timestampColumnStat.getNumValues());
    assertEquals(45, timestampColumnStat.getTotalSize());
    assertEquals(
        getInstant("2019-10-12").toEpochMilli(), timestampColumnStat.getRange().getMinValue());
    assertEquals(
        getInstant("2020-10-12").toEpochMilli(), timestampColumnStat.getRange().getMaxValue());

    ColumnStat nestedColumnStat = columnStats.get(nestedSchema.getFields().get(0));
    assertEquals(1, nestedColumnStat.getNumNulls());
    assertEquals(2, nestedColumnStat.getNumValues());
    assertEquals(2, nestedColumnStat.getRange().getMinValue());
    assertEquals(2, nestedColumnStat.getRange().getMaxValue());

    ColumnStat mapKeyColumnStat = columnStats.get(mapKeyField);
    assertEquals(1, mapKeyColumnStat.getNumNulls());
    assertEquals(3, mapKeyColumnStat.getNumValues());
    assertEquals("key1", mapKeyColumnStat.getRange().getMinValue());
    assertEquals("key2", mapKeyColumnStat.getRange().getMaxValue());

    ColumnStat mapValueColumnStat = columnStats.get(mapValueField.getSchema().getFields().get(0));
    assertEquals(1, mapValueColumnStat.getNumNulls());
    assertEquals(3, mapValueColumnStat.getNumValues());
    assertEquals(13, mapValueColumnStat.getRange().getMinValue());
    assertEquals(23, mapValueColumnStat.getRange().getMaxValue());

    ColumnStat arrayElementColumnStat = columnStats.get(arrayField.getSchema().getFields().get(0));
    assertEquals(0, arrayElementColumnStat.getNumNulls());
    assertEquals(6, arrayElementColumnStat.getNumValues());
    assertEquals(1, arrayElementColumnStat.getRange().getMinValue());
    assertEquals(6, arrayElementColumnStat.getRange().getMaxValue());
  }

  private OneField getArrayField(OneField nestedIntBase) {
    return OneField.builder()
        .name(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME)
        .parentPath("repeated_record")
        .schema(
            getNestedSchema(
                nestedIntBase, "repeated_record." + OneField.Constants.ARRAY_ELEMENT_FIELD_NAME))
        .build();
  }

  private OneField getMapValueField(OneField nestedIntBase) {
    return OneField.builder()
        .name(OneField.Constants.MAP_VALUE_FIELD_NAME)
        .parentPath("map_record")
        .schema(
            getNestedSchema(nestedIntBase, "map_record." + OneField.Constants.MAP_VALUE_FIELD_NAME))
        .build();
  }

  private OneField getMapKeyField() {
    return OneField.builder()
        .name(OneField.Constants.MAP_KEY_FIELD_NAME)
        .parentPath("map_record")
        .schema(OneSchema.builder().name("map_key").dataType(OneType.STRING).build())
        .build();
  }

  private OneField getTimestampField() {
    return OneField.builder()
        .name("timestamp_field")
        .schema(OneSchema.builder().name("time").dataType(OneType.TIMESTAMP_NTZ).build())
        .build();
  }

  private OneField getDateField() {
    return OneField.builder()
        .name("date_field")
        .schema(OneSchema.builder().name("date").dataType(OneType.DATE).build())
        .build();
  }

  private OneField getStringField() {
    return OneField.builder()
        .name("string_field")
        .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
        .build();
  }

  private OneField getLongField() {
    return OneField.builder()
        .name("long_field")
        .schema(OneSchema.builder().name("long").dataType(OneType.LONG).build())
        .build();
  }

  private OneField getNestedIntBase() {
    return OneField.builder()
        .name("nested_int")
        .schema(OneSchema.builder().name("int").dataType(OneType.INT).isNullable(false).build())
        .build();
  }

  private OneSchema getNestedSchema(OneField nestedIntBase, String parentPath) {
    return OneSchema.builder()
        .name("nested")
        .dataType(OneType.RECORD)
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
      Instant timestampValue) {
    GenericData.Record record = new GenericData.Record(AVRO_SCHEMA);
    record.put("long_field", longValue);
    record.put("string_field", stringValue);
    record.put("timestamp_field", timestampValue.toEpochMilli());
    record.put("date_field", dateValue.toLocalDate().toEpochDay());
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
