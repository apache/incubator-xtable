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
 
package org.apache.xtable.delta;

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

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;

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
  private static final InternalSchema TIMESTAMP_SCHEMA =
      InternalSchema.builder()
          .name("timestamp")
          .dataType(InternalType.TIMESTAMP)
          .metadata(
              Collections.singletonMap(
                  InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                  InternalSchema.MetadataValue.MICROS))
          .build();

  private final DeltaPartitionExtractor deltaPartitionExtractor =
      DeltaPartitionExtractor.getInstance();
  private final DeltaSchemaExtractor deltaSchemaExtractor = DeltaSchemaExtractor.getInstance();

  @Test
  public void testUnpartitionedTable() {
    StructType tableSchema =
        getSchemaWithFields(Arrays.asList("id", "firstName", "gender", "birthDate"));
    InternalSchema internalSchema = deltaSchemaExtractor.toInternalSchema(tableSchema);
    List<InternalPartitionField> internalPartitionFields =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(internalSchema, new StructType());
    assertTrue(internalPartitionFields.isEmpty());
  }

  @Test
  public void testSimplePartitionedTable() {
    StructType tableSchema =
        getSchemaWithFields(Arrays.asList("id", "firstName", "gender", "birthDate"));
    StructType partitionSchema = getSchemaWithFields(Arrays.asList("gender"));
    InternalSchema internalSchema = deltaSchemaExtractor.toInternalSchema(tableSchema);
    List<InternalPartitionField> expectedInternalPartitionFields =
        Arrays.asList(
            InternalPartitionField.builder()
                .sourceField(
                    InternalField.builder()
                        .name("gender")
                        .schema(
                            InternalSchema.builder()
                                .name("string")
                                .dataType(InternalType.STRING)
                                .build())
                        .build())
                .transformType(PartitionTransformType.VALUE)
                .build());
    List<InternalPartitionField> internalPartitionFields =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(internalSchema, partitionSchema);
    assertEquals(expectedInternalPartitionFields, internalPartitionFields);
  }

  @Test
  public void testDatePartitionedGeneratedColumnsTable() {
    StructType tableSchema =
        getSchemaWithFields(Arrays.asList("id", "firstName", "gender", "birthDate", "dateOfBirth"));
    StructType partitionSchema = getSchemaWithFields(Arrays.asList("dateOfBirth"));
    InternalSchema internalSchema = deltaSchemaExtractor.toInternalSchema(tableSchema);
    List<InternalPartitionField> expectedInternalPartitionFields =
        Arrays.asList(
            InternalPartitionField.builder()
                .sourceField(
                    InternalField.builder().name("birthDate").schema(TIMESTAMP_SCHEMA).build())
                .transformType(PartitionTransformType.DAY)
                .partitionFieldNames(Collections.singletonList("dateOfBirth"))
                .build());
    List<InternalPartitionField> internalPartitionFields =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(internalSchema, partitionSchema);
    assertEquals(expectedInternalPartitionFields, internalPartitionFields);
  }

  @Test
  public void testDateFormatPartitionedGeneratedColumnsTable() {
    StructType tableSchema =
        getSchemaWithFields(Arrays.asList("id", "firstName", "gender", "birthDate", "dateFmt"));
    StructType partitionSchema = getSchemaWithFields(Arrays.asList("dateFmt"));
    InternalSchema internalSchema = deltaSchemaExtractor.toInternalSchema(tableSchema);
    List<InternalPartitionField> expectedInternalPartitionFields =
        Arrays.asList(
            InternalPartitionField.builder()
                .sourceField(
                    InternalField.builder().name("birthDate").schema(TIMESTAMP_SCHEMA).build())
                .transformType(PartitionTransformType.HOUR)
                .partitionFieldNames(Collections.singletonList("dateFmt"))
                .build());
    List<InternalPartitionField> internalPartitionFields =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(internalSchema, partitionSchema);
    assertEquals(expectedInternalPartitionFields, internalPartitionFields);
  }

  @Test
  public void yearPartitionedGeneratedColumnsTable() {
    StructType tableSchema =
        getSchemaWithFields(Arrays.asList("id", "firstName", "gender", "birthDate", "yearOfBirth"));
    StructType partitionSchema = getSchemaWithFields(Arrays.asList("yearOfBirth"));
    InternalSchema internalSchema = deltaSchemaExtractor.toInternalSchema(tableSchema);
    List<InternalPartitionField> expectedInternalPartitionFields =
        Arrays.asList(
            InternalPartitionField.builder()
                .sourceField(
                    InternalField.builder().name("birthDate").schema(TIMESTAMP_SCHEMA).build())
                .transformType(PartitionTransformType.YEAR)
                .partitionFieldNames(Collections.singletonList("yearOfBirth"))
                .build());
    List<InternalPartitionField> internalPartitionFields =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(internalSchema, partitionSchema);
    assertEquals(expectedInternalPartitionFields, internalPartitionFields);
  }

  @Test
  public void yearAndSimpleCombinedPartitionedGeneratedColumnsTable() {
    StructType tableSchema =
        getSchemaWithFields(Arrays.asList("id", "firstName", "gender", "birthDate", "yearOfBirth"));
    StructType partitionSchema = getSchemaWithFields(Arrays.asList("yearOfBirth", "id"));
    InternalSchema internalSchema = deltaSchemaExtractor.toInternalSchema(tableSchema);
    List<InternalPartitionField> expectedInternalPartitionFields =
        Arrays.asList(
            InternalPartitionField.builder()
                .sourceField(
                    InternalField.builder().name("birthDate").schema(TIMESTAMP_SCHEMA).build())
                .transformType(PartitionTransformType.YEAR)
                .partitionFieldNames(Collections.singletonList("yearOfBirth"))
                .build(),
            InternalPartitionField.builder()
                .sourceField(
                    InternalField.builder()
                        .name("id")
                        .schema(
                            InternalSchema.builder()
                                .name("integer")
                                .dataType(InternalType.INT)
                                .build())
                        .build())
                .transformType(PartitionTransformType.VALUE)
                .build());
    List<InternalPartitionField> internalPartitionFields =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(internalSchema, partitionSchema);
    assertEquals(expectedInternalPartitionFields, internalPartitionFields);
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
    InternalSchema internalSchema = deltaSchemaExtractor.toInternalSchema(tableSchema);
    List<InternalPartitionField> expectedInternalPartitionFields =
        Arrays.asList(
            InternalPartitionField.builder()
                .sourceField(
                    InternalField.builder().name("birthDate").schema(TIMESTAMP_SCHEMA).build())
                .partitionFieldNames(
                    Arrays.asList("yearOfBirth", "monthOfBirth", "dayOfBirth", "hourOfBirth"))
                .transformType(PartitionTransformType.HOUR)
                .build());
    List<InternalPartitionField> internalPartitionFields =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(internalSchema, partitionSchema);
    assertEquals(expectedInternalPartitionFields, internalPartitionFields);
  }

  // Test for preserving order of partition columns.
  @Test
  public void testCombinationOfPlainAndGeneratedColumns() {
    StructType tableSchema =
        getSchemaWithFields(Arrays.asList("id", "firstName", "gender", "birthDate", "dateFmt"));
    StructType partitionSchema =
        getSchemaWithFields(Arrays.asList("id", "dateFmt", "gender", "dateOfBirth"));
    InternalSchema internalSchema = deltaSchemaExtractor.toInternalSchema(tableSchema);
    List<InternalPartitionField> expectedInternalPartitionFields =
        Arrays.asList(
            InternalPartitionField.builder()
                .sourceField(
                    InternalField.builder()
                        .name("id")
                        .schema(
                            InternalSchema.builder()
                                .name("integer")
                                .dataType(InternalType.INT)
                                .build())
                        .build())
                .transformType(PartitionTransformType.VALUE)
                .build(),
            InternalPartitionField.builder()
                .sourceField(
                    InternalField.builder().name("birthDate").schema(TIMESTAMP_SCHEMA).build())
                .transformType(PartitionTransformType.HOUR)
                .partitionFieldNames(Collections.singletonList("dateFmt"))
                .build(),
            InternalPartitionField.builder()
                .sourceField(
                    InternalField.builder()
                        .name("gender")
                        .schema(
                            InternalSchema.builder()
                                .name("string")
                                .dataType(InternalType.STRING)
                                .build())
                        .build())
                .transformType(PartitionTransformType.VALUE)
                .build(),
            InternalPartitionField.builder()
                .sourceField(
                    InternalField.builder().name("birthDate").schema(TIMESTAMP_SCHEMA).build())
                .transformType(PartitionTransformType.DAY)
                .partitionFieldNames(Collections.singletonList("dateOfBirth"))
                .build());
    List<InternalPartitionField> internalPartitionFields =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(internalSchema, partitionSchema);
    assertEquals(expectedInternalPartitionFields, internalPartitionFields);
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
    InternalPartitionField internalPartitionField1 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("partition_column1")
                    .schema(
                        InternalSchema.builder()
                            .name("string")
                            .dataType(InternalType.STRING)
                            .build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    InternalPartitionField internalPartitionField2 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("partition_column2")
                    .schema(
                        InternalSchema.builder()
                            .name("string")
                            .dataType(InternalType.STRING)
                            .build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    Range rangeForPartitionField1 = Range.scalar("partition_value1");
    Range rangeForPartitionField2 = Range.scalar("partition_value2");
    List<PartitionValue> expectedPartitionValues =
        Arrays.asList(
            PartitionValue.builder()
                .partitionField(internalPartitionField1)
                .range(rangeForPartitionField1)
                .build(),
            PartitionValue.builder()
                .partitionField(internalPartitionField2)
                .range(rangeForPartitionField2)
                .build());
    List<PartitionValue> partitionValues =
        deltaPartitionExtractor.partitionValueExtraction(
            scalaMap, Arrays.asList(internalPartitionField1, internalPartitionField2));
    assertEquals(expectedPartitionValues, partitionValues);
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
    InternalPartitionField internalPartitionField1 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("partition_column1")
                    .schema(
                        InternalSchema.builder()
                            .name("string")
                            .dataType(InternalType.STRING)
                            .build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    InternalPartitionField internalPartitionField2 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("some_date_column")
                    .schema(
                        InternalSchema.builder()
                            .name("timestamp")
                            .dataType(InternalType.TIMESTAMP)
                            .build())
                    .build())
            .partitionFieldNames(Collections.singletonList("date_partition_column"))
            .transformType(PartitionTransformType.HOUR)
            .build();
    Range rangeForPartitionField1 = Range.scalar("partition_value1");
    Range rangeForPartitionField2 = Range.scalar(1376992800000L);
    List<PartitionValue> expectedPartitionValues =
        Arrays.asList(
            PartitionValue.builder()
                .partitionField(internalPartitionField1)
                .range(rangeForPartitionField1)
                .build(),
            PartitionValue.builder()
                .partitionField(internalPartitionField2)
                .range(rangeForPartitionField2)
                .build());
    List<PartitionValue> partitionValues =
        deltaPartitionExtractor.partitionValueExtraction(
            scalaMap, Arrays.asList(internalPartitionField1, internalPartitionField2));
    assertEquals(expectedPartitionValues, partitionValues);
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
    InternalPartitionField internalPartitionField1 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("partition_column1")
                    .schema(
                        InternalSchema.builder()
                            .name("string")
                            .dataType(InternalType.STRING)
                            .build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    InternalPartitionField internalPartitionField2 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("some_date_column")
                    .schema(
                        InternalSchema.builder()
                            .name("timestamp")
                            .dataType(InternalType.TIMESTAMP)
                            .build())
                    .build())
            .partitionFieldNames(
                Arrays.asList(
                    "year_partition_column", "month_partition_column", "day_partition_column"))
            .transformType(PartitionTransformType.DAY)
            .build();
    Range rangeForPartitionField1 = Range.scalar("partition_value1");
    Range rangeForPartitionField2 = Range.scalar(1376956800000L);
    List<PartitionValue> expectedPartitionValues =
        Arrays.asList(
            PartitionValue.builder()
                .partitionField(internalPartitionField1)
                .range(rangeForPartitionField1)
                .build(),
            PartitionValue.builder()
                .partitionField(internalPartitionField2)
                .range(rangeForPartitionField2)
                .build());
    List<PartitionValue> partitionValues =
        deltaPartitionExtractor.partitionValueExtraction(
            scalaMap, Arrays.asList(internalPartitionField1, internalPartitionField2));
    assertEquals(expectedPartitionValues, partitionValues);
  }

  private scala.collection.mutable.Map<String, String> convertJavaMapToScalaMap(
      Map<String, String> javaMap) {
    return JavaConverters.mapAsScalaMapConverter(javaMap).asScala();
  }

  private StructType getSchemaWithFields(List<String> fields) {
    return new StructType(fields.stream().map(STRUCT_FIELD_MAP::get).toArray(StructField[]::new));
  }
}
