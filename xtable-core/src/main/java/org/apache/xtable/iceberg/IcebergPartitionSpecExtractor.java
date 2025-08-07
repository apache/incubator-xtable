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
 
package org.apache.xtable.iceberg;

import static org.apache.xtable.iceberg.IcebergPartitionValueConverter.BUCKET;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Types;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.exception.PartitionSpecException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.schema.SchemaFieldFinder;

/** Partition spec builder and extractor for Iceberg. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IcebergPartitionSpecExtractor {
  private static final Pattern NUM_BUCKETS_MATCHER = Pattern.compile("bucket\\[(\\d+)\\]");
  private static final IcebergPartitionSpecExtractor INSTANCE = new IcebergPartitionSpecExtractor();

  public static IcebergPartitionSpecExtractor getInstance() {
    return INSTANCE;
  }

  public PartitionSpec toIceberg(List<InternalPartitionField> partitionFields, Schema tableSchema) {
    if (partitionFields == null || partitionFields.isEmpty()) {
      return PartitionSpec.unpartitioned();
    }
    PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(tableSchema);
    for (InternalPartitionField partitioningField : partitionFields) {
      String fieldPath = partitioningField.getSourceField().getPath();
      switch (partitioningField.getTransformType()) {
        case YEAR:
          partitionSpecBuilder.year(fieldPath); //TODO check error here cannot find column
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
        case BUCKET:
          partitionSpecBuilder.bucket(
              fieldPath,
              (int)
                  partitioningField.getTransformOptions().get(InternalPartitionField.NUM_BUCKETS));
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

    if (transformName.startsWith(BUCKET)) {
      return PartitionTransformType.BUCKET;
    }

    throw new NotSupportedException(transform.toString());
  }

  private Map<String, Object> getPartitionTransformOptions(Transform<?, ?> transform) {
    if (transform.toString().startsWith(BUCKET)) {
      Matcher matcher = NUM_BUCKETS_MATCHER.matcher(transform.toString());
      if (matcher.matches()) {
        return Collections.singletonMap(
            InternalPartitionField.NUM_BUCKETS, Integer.parseInt(matcher.group(1)));
      } else {
        throw new PartitionSpecException(
            "Cannot parse number of buckets from partition transform: " + transform);
      }
    }
    return Collections.emptyMap();
  }

  /**
   * Generates internal representation of the Iceberg partition spec.
   *
   * @param iceSpec the Iceberg partition spec
   * @param iceSchema the Iceberg schema
   * @return generated internal representation of the Iceberg partition spec
   */
  public List<InternalPartitionField> fromIceberg(
      PartitionSpec iceSpec, Schema iceSchema, InternalSchema irSchema) {
    if (iceSpec.isUnpartitioned()) {
      return Collections.emptyList();
    }

    List<InternalPartitionField> irPartitionFields = new ArrayList<>(iceSpec.fields().size());
    for (PartitionField iceField : iceSpec.fields()) {
      // skip void transform
      if (iceField.transform().isVoid()) {
        continue;
      }
      // fetch the ice field from the schema to properly handle hidden partition fields
      int sourceColumnId = iceField.sourceId();
      Types.NestedField iceSchemaField = iceSchema.findField(sourceColumnId);

      InternalField irField =
          SchemaFieldFinder.getInstance().findFieldByPath(irSchema, iceSchemaField.name());
      InternalPartitionField irPartitionField =
          InternalPartitionField.builder()
              .sourceField(irField)
              .transformType(fromIcebergTransform(iceField.transform()))
              .transformOptions(getPartitionTransformOptions(iceField.transform()))
              .build();
      irPartitionFields.add(irPartitionField);
    }

    return irPartitionFields;
  }
}
