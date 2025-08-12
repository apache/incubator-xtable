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

package org.apache.xtable.parquet;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import lombok.AllArgsConstructor;

import org.apache.parquet.schema.Type;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.xtable.hudi.PathBasedPartitionSpecExtractor;
import org.apache.xtable.model.schema.*;
import org.apache.xtable.schema.SchemaFieldFinder;

/**
 * Parses the InternalPartitionFields from a configured list of specs with the format
 * path:type:format for date types or path:type for value types.
 */
@AllArgsConstructor
public class ParquetPartitionSpecExtractor implements PathBasedPartitionSpecExtractor {
    private static final ParquetPartitionSpecExtractor INSTANCE = new ParquetPartitionSpecExtractor(new ArrayList<>());
    private static final ParquetSchemaExtractor schemaExtractor =
            ParquetSchemaExtractor.getInstance();
    private final List<PartitionFieldSpec> partitionFieldSpecs;

    public static ParquetPartitionSpecExtractor getInstance() {
        return INSTANCE;
    }

    public static List<InternalPartitionField> inferPartitionField(InternalSchema fileSchema, String basePath) {
        List<InternalPartitionField> partitionFields = new ArrayList<>();
        URI uri = null;
        try {
            uri = new URI(basePath);
        } catch (java.net.URISyntaxException e) {
            // Handle the exception gracefully.
            // For example, you can log the error and return a default value.
            System.err.println("Invalid URI syntax: " + uri);
            return null; // or throw a new RuntimeException
        }
        Path path = Paths.get(uri);
        String absPath = path.toString();
        String[] columnNamesArray = ParquetParitionUtil.getSparkSession(Path.of(absPath)).read().parquet(absPath).columns();
        List<String> columnNamesList = new ArrayList<>(Arrays.asList(columnNamesArray));
        StructType schema = ParquetParitionUtil.getSparkSession(Path.of(absPath)).read().parquet(absPath).schema();

        for (String field : columnNamesList) {
            if (!ParquetParitionUtil.containsField(fileSchema,field)) {
                //create the sourceFields
                StructField fieldStruct = schema.apply(field);
                Type parquetFieldType = ParquetParitionUtil.convertField(fieldStruct);
                InternalSchema parquetFieldInternalType =
                        schemaExtractor.toInternalSchema(parquetFieldType, null);
                InternalField sourceField = InternalField.builder()
                        .name(fieldStruct.name())
                        .schema(parquetFieldInternalType)
                        .build();
                PartitionTransformType fieldSpec = PartitionTransformType.YEAR;// knwon to be YEAR adjust later from config
                partitionFields.add(
                        InternalPartitionField.builder()
                                .sourceField(sourceField)
                                .transformType(fieldSpec)
                                .build());
            }
        }
        return partitionFields;
    }

    @Override
    public List<InternalPartitionField> spec(InternalSchema tableSchema) {

        List<InternalPartitionField> partitionFields = new ArrayList<>(partitionFieldSpecs.size());
        for (PartitionFieldSpec fieldSpec : partitionFieldSpecs) {
            InternalField sourceField =
                    SchemaFieldFinder.getInstance()
                            .findFieldByPath(tableSchema, fieldSpec.getSourceFieldPath());
            partitionFields.add(
                    InternalPartitionField.builder()
                            .sourceField(sourceField)
                            .transformType(fieldSpec.getTransformType())
                            .build());
        }
        return partitionFields;
    }

    @Override
    public Map<String, String> getPathToPartitionFieldFormat() {
        Map<String, String> pathToPartitionFieldFormat = new HashMap<>();
        //ParquetSourceConfig partitionConfig = ParquetSourceConfig.fromPartitionFieldSpecConfig(partitionFieldSpecConfig);
        partitionFieldSpecs.forEach(
                partitionFieldSpec -> {
                    if (partitionFieldSpec.getFormat() != null) {
                        pathToPartitionFieldFormat.put(
                                partitionFieldSpec.getSourceFieldPath(), partitionFieldSpec.getFormat());
                    }
                });
        return pathToPartitionFieldFormat;
    }
}
