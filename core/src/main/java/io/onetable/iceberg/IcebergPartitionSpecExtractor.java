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
 
package io.onetable.iceberg;

import java.util.ArrayList;
import java.util.List;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;

/** Partition spec builder and extractor for Iceberg. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IcebergPartitionSpecExtractor {
  private static final IcebergPartitionSpecExtractor INSTANCE = new IcebergPartitionSpecExtractor();

  public static IcebergPartitionSpecExtractor getInstance() {
    return INSTANCE;
  }

  public PartitionSpec toIceberg(List<OnePartitionField> partitionFields, Schema tableSchema) {
    if (partitionFields == null || partitionFields.isEmpty()) {
      return PartitionSpec.unpartitioned();
    }
    PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(tableSchema);
    for (OnePartitionField partitioningField : partitionFields) {
      String fieldPath = partitioningField.getSourceField().getPath();
      switch (partitioningField.getTransformType()) {
        case YEAR:
          partitionSpecBuilder.year(fieldPath);
          break;
        case MONTH:
          partitionSpecBuilder.month(fieldPath);
          break;
        case DAY:
          partitionSpecBuilder.day(fieldPath);
          break;
        case HOUR:
          partitionSpecBuilder.hour(fieldPath);
          break;
        case VALUE:
          partitionSpecBuilder.identity(fieldPath);
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported type: " + partitioningField.getTransformType());
      }
    }
    return partitionSpecBuilder.build();
  }

  public List<OnePartitionField> fromIceberg(PartitionSpec iceSpec) {
    List<OnePartitionField> irPartitionFields = new ArrayList<>();

    if (iceSpec == null || iceSpec.fields().isEmpty()) {
      return irPartitionFields;
    }

    for (PartitionField iceField : iceSpec.fields()) {
      OneField irField =
          OneField.builder().name(iceField.name()).fieldId(iceField.sourceId()).build();

      OnePartitionField partitionField = OnePartitionField.builder().sourceField(irField).build();
      irPartitionFields.add(partitionField);
    }

    return irPartitionFields;
  }
}
