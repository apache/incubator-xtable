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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.schema.PartitionTransformType;

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
                .build());
    List<OnePartitionField> onePartitionFields =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(oneSchema, partitionSchema);
    assertEquals(expectedOnePartitionFields, onePartitionFields);
  }

  private StructType getSchemaWithFields(List<String> fields) {
    return new StructType(fields.stream().map(STRUCT_FIELD_MAP::get).toArray(StructField[]::new));
  }
}
