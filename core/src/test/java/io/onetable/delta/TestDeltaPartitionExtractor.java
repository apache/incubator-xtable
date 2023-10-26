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
 
package io.onetable.delta;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import scala.collection.JavaConverters;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.stat.Range;

/** Validates the partition extraction logic from Delta tables. */
public class TestDeltaPartitionExtractor {
  private static final Map<String, StructField> STRUCT_FIELD_MAP =
      new HashMap<String, StructField>() {
        {
          put("id", DataTypes.createStructField("id", DataTypes.IntegerType, false));
          put("firstName", DataTypes.createStructField("firstName", DataTypes.StringType, false));
          put("gender", DataTypes.createStructField("gender", DataTypes.StringType, false));
          put(
              "birthDate",
              DataTypes.createStructField("birthDate", DataTypes.TimestampType, false));
          put(
              "dateOfBirth",
              DataTypes.createStructField(
                  "dateOfBirth",
                  DataTypes.DateType,
                  false,
                  Metadata.fromJson(
                      "{\"delta.generationExpression\": \"CAST(birthDate AS DATE)\"}")));
          put(
              "dateFmt",
              DataTypes.createStructField(
                  "dateFmt",
                  DataTypes.StringType,
                  false,
                  Metadata.fromJson(
                      "{\"delta.generationExpression\": \"DATE_FORMAT(birthDate, 'yyyy-MM-dd-HH')\"}")));
          put(
              "yearOfBirth",
              DataTypes.createStructField(
                  "yearOfBirth",
                  DataTypes.IntegerType,
                  false,
                  Metadata.fromJson("{\"delta.generationExpression\": \"YEAR(birthDate)\"}")));
          put(
              "monthOfBirth",
              DataTypes.createStructField(
                  "monthOfBirth",
                  DataTypes.IntegerType,
                  false,
                  Metadata.fromJson("{\"delta.generationExpression\": \"MONTH(birthDate)\"}")));
          put(
              "dayOfBirth",
              DataTypes.createStructField(
                  "dayOfBirth",
                  DataTypes.IntegerType,
                  false,
                  Metadata.fromJson("{\"delta.generationExpression\": \"DAY(birthDate)\"}")));
          put(
              "hourOfBirth",
              DataTypes.createStructField(
                  "hourOfBirth",
                  DataTypes.IntegerType,
                  false,
                  Metadata.fromJson("{\"delta.generationExpression\": \"HOUR(birthDate)\"}")));
        }
      };

  private final DeltaPartitionExtractor deltaPartitionExtractor =
      DeltaPartitionExtractor.getInstance();
  private final DeltaSchemaExtractor deltaSchemaExtractor = DeltaSchemaExtractor.getInstance();

  @Test
  public void testUnpartitionedTable() {
    StructType tableSchema =
        getSchemaWithFields(Arrays.asList("id", "firstName", "gender", "birthDate"));
    OneSchema oneSchema = deltaSchemaExtractor.toOneSchema(tableSchema);
    List<OnePartitionField> onePartitionFields =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(oneSchema, new StructType());
    assertTrue(onePartitionFields.isEmpty());
  }

  @Test
  public void testSimplePartitionedTable() {
    StructType tableSchema =
        getSchemaWithFields(Arrays.asList("id", "firstName", "gender", "birthDate"));
    StructType partitionSchema = getSchemaWithFields(Arrays.asList("gender"));
    OneSchema oneSchema = deltaSchemaExtractor.toOneSchema(tableSchema);
    List<OnePartitionField> expectedOnePartitionFields =
        Arrays.asList(
            OnePartitionField.builder()
                .sourceField(
                    OneField.builder()
                        .name("gender")
                        .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
                        .build())
                .transformType(PartitionTransformType.VALUE)
                .build());
    List<OnePartitionField> onePartitionFields =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(oneSchema, partitionSchema);
    assertEquals(expectedOnePartitionFields, onePartitionFields);
  }

  @Test
  public void testDatePartitionedGeneratedColumnsTable() {
    StructType tableSchema =
        getSchemaWithFields(Arrays.asList("id", "firstName", "gender", "birthDate", "dateOfBirth"));
    StructType partitionSchema = getSchemaWithFields(Arrays.asList("dateOfBirth"));
    OneSchema oneSchema = deltaSchemaExtractor.toOneSchema(tableSchema);
    List<OnePartitionField> expectedOnePartitionFields =
        Arrays.asList(
            OnePartitionField.builder()
                .sourceField(
                    OneField.builder()
                        .name("birthDate")
                        .schema(
                            OneSchema.builder()
                                .name("timestamp")
                                .dataType(OneType.TIMESTAMP)
                                .build())
                        .build())
                .transformType(PartitionTransformType.DAY)
                .partitionFieldNames(Collections.singletonList("dateOfBirth"))
                .build());
    List<OnePartitionField> onePartitionFields =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(oneSchema, partitionSchema);
    assertEquals(expectedOnePartitionFields, onePartitionFields);
  }

  @Test
  public void testDateFormatPartitionedGeneratedColumnsTable() {
    StructType tableSchema =
        getSchemaWithFields(Arrays.asList("id", "firstName", "gender", "birthDate", "dateFmt"));
    StructType partitionSchema = getSchemaWithFields(Arrays.asList("dateFmt"));
    OneSchema oneSchema = deltaSchemaExtractor.toOneSchema(tableSchema);
    List<OnePartitionField> expectedOnePartitionFields =
        Arrays.asList(
            OnePartitionField.builder()
                .sourceField(
                    OneField.builder()
                        .name("birthDate")
                        .schema(
                            OneSchema.builder()
                                .name("timestamp")
                                .dataType(OneType.TIMESTAMP)
                                .build())
                        .build())
                .transformType(PartitionTransformType.HOUR)
                .partitionFieldNames(Collections.singletonList("dateFmt"))
                .build());
    List<OnePartitionField> onePartitionFields =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(oneSchema, partitionSchema);
    assertEquals(expectedOnePartitionFields, onePartitionFields);
  }

  @Test
  public void yearPartitionedGeneratedColumnsTable() {
    StructType tableSchema =
        getSchemaWithFields(Arrays.asList("id", "firstName", "gender", "birthDate", "yearOfBirth"));
    StructType partitionSchema = getSchemaWithFields(Arrays.asList("yearOfBirth"));
    OneSchema oneSchema = deltaSchemaExtractor.toOneSchema(tableSchema);
    List<OnePartitionField> expectedOnePartitionFields =
        Arrays.asList(
            OnePartitionField.builder()
                .sourceField(
                    OneField.builder()
                        .name("birthDate")
                        .schema(
                            OneSchema.builder()
                                .name("timestamp")
                                .dataType(OneType.TIMESTAMP)
                                .build())
                        .build())
                .transformType(PartitionTransformType.YEAR)
                .partitionFieldNames(Collections.singletonList("yearOfBirth"))
                .build());
    List<OnePartitionField> onePartitionFields =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(oneSchema, partitionSchema);
    assertEquals(expectedOnePartitionFields, onePartitionFields);
  }

  @Test
  public void yearAndSimpleCombinedPartitionedGeneratedColumnsTable() {
    StructType tableSchema =
        getSchemaWithFields(Arrays.asList("id", "firstName", "gender", "birthDate", "yearOfBirth"));
    StructType partitionSchema = getSchemaWithFields(Arrays.asList("yearOfBirth", "id"));
    OneSchema oneSchema = deltaSchemaExtractor.toOneSchema(tableSchema);
    List<OnePartitionField> expectedOnePartitionFields =
        Arrays.asList(
            OnePartitionField.builder()
                .sourceField(
                    OneField.builder()
                        .name("birthDate")
                        .schema(
                            OneSchema.builder()
                                .name("timestamp")
                                .dataType(OneType.TIMESTAMP)
                                .build())
                        .build())
                .transformType(PartitionTransformType.YEAR)
                .partitionFieldNames(Collections.singletonList("yearOfBirth"))
                .build(),
            OnePartitionField.builder()
                .sourceField(
                    OneField.builder()
                        .name("id")
                        .schema(OneSchema.builder().name("integer").dataType(OneType.INT).build())
                        .build())
                .transformType(PartitionTransformType.VALUE)
                .build());
    List<OnePartitionField> onePartitionFields =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(oneSchema, partitionSchema);
    assertEquals(expectedOnePartitionFields, onePartitionFields);
  }

  @Test
  public void yearMonthDayHourPartitionedGeneratedColumnsTable() {
    StructType tableSchema =
        getSchemaWithFields(
            Arrays.asList(
                "id",
                "firstName",
                "gender",
                "birthDate",
                "yearOfBirth",
                "monthOfBirth",
                "dayOfBirth",
                "hourOfBirth"));
    StructType partitionSchema =
        getSchemaWithFields(
            Arrays.asList("yearOfBirth", "monthOfBirth", "dayOfBirth", "hourOfBirth"));
    OneSchema oneSchema = deltaSchemaExtractor.toOneSchema(tableSchema);
    List<OnePartitionField> expectedOnePartitionFields =
        Arrays.asList(
            OnePartitionField.builder()
                .sourceField(
                    OneField.builder()
                        .name("birthDate")
                        .schema(
                            OneSchema.builder()
                                .name("timestamp")
                                .dataType(OneType.TIMESTAMP)
                                .build())
                        .build())
                .partitionFieldNames(
                    Arrays.asList("yearOfBirth", "monthOfBirth", "dayOfBirth", "hourOfBirth"))
                .transformType(PartitionTransformType.HOUR)
                .build());
    List<OnePartitionField> onePartitionFields =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(oneSchema, partitionSchema);
    assertEquals(expectedOnePartitionFields, onePartitionFields);
  }

  // Test for preserving order of partition columns.
  @Test
  public void testCombinationOfPlainAndGeneratedColumns() {
    StructType tableSchema =
        getSchemaWithFields(Arrays.asList("id", "firstName", "gender", "birthDate", "dateFmt"));
    StructType partitionSchema =
        getSchemaWithFields(Arrays.asList("id", "dateFmt", "gender", "dateOfBirth"));
    OneSchema oneSchema = deltaSchemaExtractor.toOneSchema(tableSchema);
    List<OnePartitionField> expectedOnePartitionFields =
        Arrays.asList(
            OnePartitionField.builder()
                .sourceField(
                    OneField.builder()
                        .name("id")
                        .schema(OneSchema.builder().name("integer").dataType(OneType.INT).build())
                        .build())
                .transformType(PartitionTransformType.VALUE)
                .build(),
            OnePartitionField.builder()
                .sourceField(
                    OneField.builder()
                        .name("birthDate")
                        .schema(
                            OneSchema.builder()
                                .name("timestamp")
                                .dataType(OneType.TIMESTAMP)
                                .build())
                        .build())
                .transformType(PartitionTransformType.HOUR)
                .partitionFieldNames(Collections.singletonList("dateFmt"))
                .build(),
            OnePartitionField.builder()
                .sourceField(
                    OneField.builder()
                        .name("gender")
                        .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
                        .build())
                .transformType(PartitionTransformType.VALUE)
                .build(),
            OnePartitionField.builder()
                .sourceField(
                    OneField.builder()
                        .name("birthDate")
                        .schema(
                            OneSchema.builder()
                                .name("timestamp")
                                .dataType(OneType.TIMESTAMP)
                                .build())
                        .build())
                .transformType(PartitionTransformType.DAY)
                .partitionFieldNames(Collections.singletonList("dateOfBirth"))
                .build());
    List<OnePartitionField> onePartitionFields =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(oneSchema, partitionSchema);
    assertEquals(expectedOnePartitionFields, onePartitionFields);
  }

  @Test
  public void testSimplePartitionValueExtraction() {
    Map<String, String> partitionValuesMap =
        new HashMap<String, String>() {
          {
            put("partition_column1", "partition_value1");
            put("partition_column2", "partition_value2");
          }
        };
    scala.collection.mutable.Map<String, String> scalaMap =
        convertJavaMapToScalaMap(partitionValuesMap);
    OnePartitionField onePartitionField1 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("partition_column1")
                    .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    OnePartitionField onePartitionField2 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("partition_column2")
                    .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    Range rangeForPartitionField1 = Range.scalar("partition_value1");
    Range rangeForPartitionField2 = Range.scalar("partition_value2");
    Map<OnePartitionField, Range> expectedPartitionFieldRangeMap =
        new HashMap<OnePartitionField, Range>() {
          {
            put(onePartitionField1, rangeForPartitionField1);
            put(onePartitionField2, rangeForPartitionField2);
          }
        };
    Map<OnePartitionField, Range> partitionFieldRangeMap =
        deltaPartitionExtractor.partitionValueExtraction(
            scalaMap, Arrays.asList(onePartitionField1, onePartitionField2));
    assertEquals(expectedPartitionFieldRangeMap, partitionFieldRangeMap);
  }

  @Test
  public void testDateFormatGeneratedPartitionValueExtraction() {
    // date_partition_column is generated in the table as DATE_FORMAT(some_date_column,
    // 'yyyy-MM-dd-HH')
    // where some_date_column is of timestamp type.
    Map<String, String> partitionValuesMap =
        new HashMap<String, String>() {
          {
            put("partition_column1", "partition_value1");
            put("date_partition_column", "2013-08-20-10");
          }
        };
    scala.collection.mutable.Map<String, String> scalaMap =
        convertJavaMapToScalaMap(partitionValuesMap);
    OnePartitionField onePartitionField1 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("partition_column1")
                    .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    OnePartitionField onePartitionField2 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("some_date_column")
                    .schema(
                        OneSchema.builder().name("timestamp").dataType(OneType.TIMESTAMP).build())
                    .build())
            .partitionFieldNames(Collections.singletonList("date_partition_column"))
            .transformType(PartitionTransformType.HOUR)
            .build();
    Range rangeForPartitionField1 = Range.scalar("partition_value1");
    Range rangeForPartitionField2 = Range.scalar(1376992800000L);
    Map<OnePartitionField, Range> expectedPartitionFieldRangeMap =
        new HashMap<OnePartitionField, Range>() {
          {
            put(onePartitionField1, rangeForPartitionField1);
            put(onePartitionField2, rangeForPartitionField2);
          }
        };
    Map<OnePartitionField, Range> partitionFieldRangeMap =
        deltaPartitionExtractor.partitionValueExtraction(
            scalaMap, Arrays.asList(onePartitionField1, onePartitionField2));
    assertEquals(expectedPartitionFieldRangeMap, partitionFieldRangeMap);
  }

  @Test
  public void testYearMonthDayHourGeneratedPartitionValueExtraction() {
    // year, month and day are generated in the table as based on some_date_column which is of
    // timestamp type.
    Map<String, String> partitionValuesMap =
        new HashMap<String, String>() {
          {
            put("partition_column1", "partition_value1");
            put("year_partition_column", "2013");
            put("month_partition_column", "8");
            put("day_partition_column", "20");
          }
        };
    scala.collection.mutable.Map<String, String> scalaMap =
        convertJavaMapToScalaMap(partitionValuesMap);
    OnePartitionField onePartitionField1 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("partition_column1")
                    .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    OnePartitionField onePartitionField2 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("some_date_column")
                    .schema(
                        OneSchema.builder().name("timestamp").dataType(OneType.TIMESTAMP).build())
                    .build())
            .partitionFieldNames(
                Arrays.asList(
                    "year_partition_column", "month_partition_column", "day_partition_column"))
            .transformType(PartitionTransformType.DAY)
            .build();
    Range rangeForPartitionField1 = Range.scalar("partition_value1");
    Range rangeForPartitionField2 = Range.scalar(1376956800000L);
    Map<OnePartitionField, Range> expectedPartitionFieldRangeMap =
        new HashMap<OnePartitionField, Range>() {
          {
            put(onePartitionField1, rangeForPartitionField1);
            put(onePartitionField2, rangeForPartitionField2);
          }
        };
    Map<OnePartitionField, Range> partitionFieldRangeMap =
        deltaPartitionExtractor.partitionValueExtraction(
            scalaMap, Arrays.asList(onePartitionField1, onePartitionField2));
    assertEquals(expectedPartitionFieldRangeMap, partitionFieldRangeMap);
  }

  private scala.collection.mutable.Map<String, String> convertJavaMapToScalaMap(
      Map<String, String> javaMap) {
    return JavaConverters.mapAsScalaMapConverter(javaMap).asScala();
  }

  private StructType getSchemaWithFields(List<String> fields) {
    return new StructType(fields.stream().map(STRUCT_FIELD_MAP::get).toArray(StructField[]::new));
  }
}
