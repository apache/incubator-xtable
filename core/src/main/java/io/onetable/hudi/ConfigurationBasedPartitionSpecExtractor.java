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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.schema.SchemaFieldFinder;

/**
 * Parses the OnePartitionFields from a configured list of specs with the format path:type:format
 * for date types or path:type for value types.
 */
@AllArgsConstructor
public class ConfigurationBasedPartitionSpecExtractor implements HudiSourcePartitionSpecExtractor {
  private final HudiSourceConfig config;

  @Override
  public List<OnePartitionField> spec(OneSchema tableSchema) {
    List<OnePartitionField> partitionFields =
        new ArrayList<>(config.getPartitionFieldSpecs().size());
    for (HudiSourceConfig.PartitionFieldSpec fieldSpec : config.getPartitionFieldSpecs()) {
      OneField sourceField =
          SchemaFieldFinder.getInstance()
              .findFieldByPath(tableSchema, fieldSpec.getSourceFieldPath());
      partitionFields.add(
          OnePartitionField.builder()
              .sourceField(sourceField)
              .transformType(fieldSpec.getTransformType())
              .build());
    }
    return partitionFields;
  }

  @Override
  public Map<String, String> getPathToPartitionFieldFormat() {
    Map<String, String> pathToPartitionFieldFormat = new HashMap<>();
    config
        .getPartitionFieldSpecs()
        .forEach(
            partitionFieldSpec -> {
              if (partitionFieldSpec.getFormat() != null) {
                pathToPartitionFieldFormat.put(
                    partitionFieldSpec.getSourceFieldPath(), partitionFieldSpec.getFormat());
              }
            });
    return pathToPartitionFieldFormat;
  }
}
