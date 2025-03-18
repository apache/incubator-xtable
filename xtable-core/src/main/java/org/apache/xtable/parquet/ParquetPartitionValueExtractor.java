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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.xtable.model.config.InputPartitionField;
import org.apache.xtable.model.config.InputPartitionFields;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;

/**
 * Partition value extractor for Parquet.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ParquetPartitionValueExtractor {
    private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
    private static final ParquetPartitionValueExtractor INSTANCE =
            new ParquetPartitionValueExtractor();

    public static ParquetPartitionValueExtractor getInstance() {
        return INSTANCE;
    }

    public List<InternalPartitionField> getInternalPartitionFields(InputPartitionFields partitions) {
        List<InternalPartitionField> partitionFields = new ArrayList<>();
        String sourceField = partitions.getSourceField();
        for (InputPartitionField partition : partitions.getPartitions()) {
            partitionFields.add(
                    InternalPartitionField.builder()
                            // TODO check the sourceField dataType (from the user config of the partitions)
                            .sourceField(
                                    InternalField.builder()
                                            .name(sourceField)
                                            .schema(
                                                    InternalSchema.builder()
                                                            .name(sourceField)
                                                            .dataType(InternalType.STRING)
                                                            .build())
                                            .build())
                            .transformType(partition.getTransformType())
                            .build());
        }
        return partitionFields;
    }

    public List<PartitionValue> createPartitionValues(
            Map<InternalPartitionField, Range> extractedPartitions) {
        return extractedPartitions.entrySet()
                .stream()
                .map(internalPartitionField ->
                        PartitionValue.builder()
                                .partitionField(
                                        internalPartitionField.getKey())
                                .range(internalPartitionField.getValue())
                                .build())
                .collect(Collectors.toList());
    }

    public Map<InternalPartitionField, Range> extractPartitionValues(
            InputPartitionFields partitionsConf) {
        Map<InternalPartitionField, Range> partitionValues = new HashMap<>();
        List<InputPartitionField> partitions = partitionsConf.getPartitions();
        InternalPartitionField internalPartitionField = null;
        InputPartitionField partitionField = null;
        Object value = null;
        for (int i = 0; i < partitions.size(); i++) {
            partitionField = partitions.get(i);
            // Convert date based partitions into millis since epoch
            switch (partitionField.getTransformType()) {
                case YEAR:
                    value = EPOCH.plusYears(Long.parseLong(partitionField.getPartitionValue())).toInstant().toEpochMilli();
                    break;
                case MONTH:
                    value = EPOCH.plusMonths(Long.parseLong(partitionField.getPartitionValue())).toInstant().toEpochMilli();
                    break;
                case DAY:
                    value = EPOCH.plusDays(Long.parseLong(partitionField.getPartitionValue())).toInstant().toEpochMilli();
                    break;
                case HOUR:
                    value = EPOCH.plusHours(Long.parseLong(partitionField.getPartitionValue())).toInstant().toEpochMilli();
                    break;
                default:
                    value = ((Object) partitionField.getPartitionValue());
            }
        }
        internalPartitionField = InternalPartitionField.builder()
                .sourceField(
                        InternalField.builder()
                                .name(partitionsConf.getSourceField())
                                .schema(
                                        InternalSchema.builder()
                                                .name(partitionsConf.getSourceField())
                                                // TODO check type
                                                .dataType(InternalType.STRING)
                                                .build())
                                .build())
                .transformType(partitionField.getTransformType())
                .build();
        partitionValues.put(internalPartitionField, Range.scalar(value));
        return partitionValues;
    }
}
