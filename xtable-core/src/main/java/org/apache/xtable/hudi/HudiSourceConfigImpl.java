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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import lombok.Builder;
import lombok.Value;

import com.google.common.base.Preconditions;

import org.apache.xtable.conversion.HudiSourceConfig;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.reflection.ReflectionUtils;

/** Configuration of Hudi source format for the sync process. */
@Value
public class HudiSourceConfigImpl implements HudiSourceConfig {
  String partitionSpecExtractorClass;
  List<PartitionFieldSpec> partitionFieldSpecs;

  @Builder
  public HudiSourceConfigImpl(String partitionSpecExtractorClass, String partitionFieldSpecConfig) {
    this.partitionSpecExtractorClass =
        partitionSpecExtractorClass == null
            ? ConfigurationBasedPartitionSpecExtractor.class.getName()
            : partitionSpecExtractorClass;
    this.partitionFieldSpecs = parsePartitionFieldSpecs(partitionFieldSpecConfig);
  }

  @Value
  static class PartitionFieldSpec {
    String sourceFieldPath;
    PartitionTransformType transformType;
    String format;
  }

  private static List<PartitionFieldSpec> parsePartitionFieldSpecs(String input) {
    if (input == null || input.isEmpty()) {
      return Collections.emptyList();
    }
    String[] perFieldConfigs = input.split(",");
    List<PartitionFieldSpec> partitionFields = new ArrayList<>(perFieldConfigs.length);
    for (String fieldConfig : perFieldConfigs) {
      String[] parts = fieldConfig.split(":");
      String path = parts[0];
      PartitionTransformType type =
          PartitionTransformType.valueOf(parts[1].toUpperCase(Locale.ROOT));
      String format = parts.length == 3 ? parts[2] : null;

      partitionFields.add(new PartitionFieldSpec(path, type, format));
    }
    return partitionFields;
  }

  public HudiSourcePartitionSpecExtractor loadSourcePartitionSpecExtractor() {
    Preconditions.checkNotNull(
        partitionSpecExtractorClass, "HudiSourcePartitionSpecExtractor class not provided");
    return ReflectionUtils.createInstanceOfClass(partitionSpecExtractorClass, this);
  }
}
