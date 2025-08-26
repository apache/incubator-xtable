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

import java.util.*;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;

import org.apache.xtable.hudi.PathBasedPartitionSpecExtractor;
import org.apache.xtable.model.schema.*;
import org.apache.xtable.schema.SchemaFieldFinder;

/**
 * Parses the InternalPartitionFields from a configured list of specs with the format
 * path:type:format for date types or path:type for value types.
 */
@AllArgsConstructor
public class ParquetPartitionSpecExtractor implements PathBasedPartitionSpecExtractor {
  private static final ParquetPartitionSpecExtractor INSTANCE =
      new ParquetPartitionSpecExtractor(new ArrayList<>());
  private List<PartitionFieldSpec> partitionFieldSpecs;

  public static ParquetPartitionSpecExtractor getInstance() {
    return INSTANCE;
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
              .partitionFieldNames(getListPartitionNamesFromFormatInput(fieldSpec.getFormat()))
              .transformType(fieldSpec.getTransformType())
              .build());
    }
    return partitionFields;
  }

  public List<String> getListPartitionNamesFromFormatInput(String inputFormat) {
    return Arrays.stream(inputFormat.split("/"))
        .map(s -> s.split("=")[0])
        .collect(Collectors.toList());
  }

  @Override
  public Map<String, String> getPathToPartitionFieldFormat() {
    Map<String, String> pathToPartitionFieldFormat = new HashMap<>();

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
