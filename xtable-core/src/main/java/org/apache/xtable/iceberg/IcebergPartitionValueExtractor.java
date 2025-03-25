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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.iceberg.StructLike;

import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.stat.Range;

/** Partition value extractor for Iceberg. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IcebergPartitionValueExtractor {
  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final IcebergPartitionValueExtractor INSTANCE =
      new IcebergPartitionValueExtractor();

  public static IcebergPartitionValueExtractor getInstance() {
    return INSTANCE;
  }

  public Map<InternalPartitionField, Range> extractPartitionValues(
      List<InternalPartitionField> partitionFields, StructLike structLike) {
    Map<InternalPartitionField, Range> partitionValues = new HashMap<>();
    for (int i = 0; i < structLike.size(); i++) {
      InternalPartitionField partitionField = partitionFields.get(i);
      Object value;
      // Convert date based partitions into millis since epoch
      switch (partitionField.getTransformType()) {
        case YEAR:
          value = EPOCH.plusYears(structLike.get(i, Integer.class)).toInstant().toEpochMilli();
          break;
        case MONTH:
          value = EPOCH.plusMonths(structLike.get(i, Integer.class)).toInstant().toEpochMilli();
          break;
        case DAY:
          value = EPOCH.plusDays(structLike.get(i, Integer.class)).toInstant().toEpochMilli();
          break;
        case HOUR:
          value = EPOCH.plusHours(structLike.get(i, Integer.class)).toInstant().toEpochMilli();
          break;
        default:
          value = structLike.get(i, Object.class);
      }

      partitionValues.put(partitionFields.get(i), Range.scalar(value));
    }
    return partitionValues;
  }
}
