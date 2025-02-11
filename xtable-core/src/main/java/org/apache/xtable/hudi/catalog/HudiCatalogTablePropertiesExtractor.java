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
 
package org.apache.xtable.hudi.catalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.spark.sql.types.StructType;

import org.apache.hudi.common.util.StringUtils;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.schema.SparkSchemaExtractor;

/** Util class to fetch details about Hudi table */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HudiCatalogTablePropertiesExtractor {

  private static final HudiCatalogTablePropertiesExtractor INSTANCE =
      new HudiCatalogTablePropertiesExtractor();

  public static HudiCatalogTablePropertiesExtractor getInstance() {
    return INSTANCE;
  }
  /**
   * Get Spark Sql related table properties. This is used for spark datasource table.
   *
   * @param schema The schema to write to the table.
   * @return A new parameters added the spark's table properties.
   */
  public Map<String, String> getSparkTableProperties(
      List<String> partitionNames,
      String sparkVersion,
      int schemaLengthThreshold,
      InternalSchema schema) {
    List<InternalField> partitionCols = new ArrayList<>();
    List<InternalField> dataCols = new ArrayList<>();
    Map<String, InternalField> column2Field = new HashMap<>();

    for (InternalField field : schema.getFields()) {
      column2Field.put(field.getName(), field);
    }
    // Get partition columns and data columns.
    for (String partitionName : partitionNames) {
      // Default the unknown partition fields to be String.
      // Keep the same logical with HiveSchemaUtil#getPartitionKeyType.
      partitionCols.add(
          column2Field.getOrDefault(
              partitionName,
              InternalField.builder()
                  .name(partitionName)
                  .schema(
                      InternalSchema.builder()
                          .dataType(InternalType.BYTES)
                          .isNullable(false)
                          .build())
                  .build()));
    }

    for (InternalField field : schema.getFields()) {
      if (!partitionNames.contains(field.getName())) {
        dataCols.add(field);
      }
    }

    List<InternalField> reOrderedFields = new ArrayList<>();
    reOrderedFields.addAll(dataCols);
    reOrderedFields.addAll(partitionCols);
    InternalSchema reorderedSchema =
        InternalSchema.builder()
            .fields(reOrderedFields)
            .dataType(InternalType.RECORD)
            .name(schema.getName())
            .build();

    StructType sparkSchema = SparkSchemaExtractor.getInstance().fromInternalSchema(reorderedSchema);

    Map<String, String> sparkProperties = new HashMap<>();
    sparkProperties.put("spark.sql.sources.provider", "hudi");
    if (!StringUtils.isNullOrEmpty(sparkVersion)) {
      sparkProperties.put("spark.sql.create.version", sparkVersion);
    }
    // Split the schema string to multi-parts according the schemaLengthThreshold size.
    String schemaString = sparkSchema.json();
    int numSchemaPart = (schemaString.length() + schemaLengthThreshold - 1) / schemaLengthThreshold;
    sparkProperties.put("spark.sql.sources.schema.numParts", String.valueOf(numSchemaPart));
    // Add each part of schema string to sparkProperties
    for (int i = 0; i < numSchemaPart; i++) {
      int start = i * schemaLengthThreshold;
      int end = Math.min(start + schemaLengthThreshold, schemaString.length());
      sparkProperties.put("spark.sql.sources.schema.part." + i, schemaString.substring(start, end));
    }
    // Add partition columns
    if (!partitionNames.isEmpty()) {
      sparkProperties.put(
          "spark.sql.sources.schema.numPartCols", String.valueOf(partitionNames.size()));
      for (int i = 0; i < partitionNames.size(); i++) {
        sparkProperties.put("spark.sql.sources.schema.partCol." + i, partitionNames.get(i));
      }
    }
    return sparkProperties;
  }
}
