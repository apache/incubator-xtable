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
import java.util.Collections;
import java.util.List;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.Transform;

import io.onetable.exception.NotSupportedException;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.schema.SchemaFieldFinder;

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

  PartitionTransformType fromIcebergTransform(Transform<?, ?> transform) {
    if (transform.isIdentity()) {
      return PartitionTransformType.VALUE;
    }

    String transformName = transform.toString();
    switch (transformName) {
      case "year":
        return PartitionTransformType.YEAR;
      case "month":
        return PartitionTransformType.MONTH;
      case "day":
        return PartitionTransformType.DAY;
      case "hour":
        return PartitionTransformType.HOUR;
    }

    if (transform.isVoid()) {
      throw new NotSupportedException(transformName);
    }

    if (transformName.startsWith("bucket")) {
      throw new NotSupportedException(transformName);
    }

    throw new NotSupportedException(transform.toString());
  }

  public List<OnePartitionField> fromIceberg(PartitionSpec iceSpec, OneSchema irSchema) {
    if (iceSpec.isUnpartitioned()) {
      return Collections.emptyList();
    }

    List<OnePartitionField> irPartitionFields = new ArrayList<>();
    for (PartitionField iceField : iceSpec.fields()) {
      OneField irField = SchemaFieldFinder.getInstance().findFieldByPath(irSchema, iceField.name());
      OnePartitionField irPartitionField =
          OnePartitionField.builder()
              .sourceField(irField)
              .transformType(fromIcebergTransform(iceField.transform()))
              .build();
      irPartitionFields.add(irPartitionField);
    }

    return irPartitionFields;
  }
}
