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
 
package org.apache.xtable.paimon;

import static org.apache.xtable.delta.DeltaValueConverter.convertFromDeltaPartitionValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import org.apache.xtable.exception.ReadException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;

/** Extracts partition spec for Paimon as identity transforms on partition keys. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PaimonPartitionExtractor {

  private static final PaimonPartitionExtractor INSTANCE = new PaimonPartitionExtractor();

  public static PaimonPartitionExtractor getInstance() {
    return INSTANCE;
  }

  public List<InternalPartitionField> toInternalPartitionFields(
      List<String> partitionKeys, InternalSchema schema) {
    if (partitionKeys == null || partitionKeys.isEmpty()) {
      return Collections.emptyList();
    }
    return partitionKeys.stream()
        .map(key -> toPartitionField(key, schema))
        .collect(Collectors.toList());
  }

  public List<PartitionValue> toPartitionValues(
      FileStoreTable table, BinaryRow partition, InternalSchema internalSchema) {
    InternalRowPartitionComputer partitionComputer = newPartitionComputer(table);
    Map<String, String> partValues = partitionComputer.generatePartValues(partition);

    if (partValues.isEmpty()) {
      return Collections.emptyList();
    }

    String defaultPartitionName = table.coreOptions().partitionDefaultName();
    List<PartitionValue> partitionValues = new ArrayList<>(partValues.size());
    for (Map.Entry<String, String> entry : partValues.entrySet()) {
      InternalPartitionField partitionField = toPartitionField(entry.getKey(), internalSchema);
      Object typedValue =
          convertPartitionValue(
              entry.getValue(),
              partitionField.getSourceField().getSchema().getDataType(),
              defaultPartitionName);
      partitionValues.add(
          PartitionValue.builder()
              .partitionField(partitionField)
              .range(Range.scalar(typedValue))
              .build());
    }
    return partitionValues;
  }

  /**
   * Converts a Paimon partition path value (always rendered as a String by the partition computer)
   * into the typed value the Delta and Iceberg targets expect (for example an epoch-day Integer for
   * DATE, an Integer for INT, a Long for BIGINT), reusing the same conversion the Delta sources
   * use. A null partition value is rendered by Paimon as the configured default-partition name, so
   * that sentinel is mapped back to {@code null}.
   */
  static Object convertPartitionValue(
      String rawValue, InternalType fieldType, String defaultPartitionName) {
    if (rawValue == null || rawValue.equals(defaultPartitionName)) {
      return null;
    }
    return convertFromDeltaPartitionValue(rawValue, fieldType, PartitionTransformType.VALUE, null);
  }

  public Optional<String> toPartitionPath(FileStoreTable table, BinaryRow partition) {
    InternalRowPartitionComputer partitionComputer = newPartitionComputer(table);
    return partitionComputer.generatePartValues(partition).entrySet().stream()
        .map(e -> e.getKey() + "=" + e.getValue())
        .reduce((a, b) -> a + "/" + b);
  }

  private InternalPartitionField toPartitionField(String key, InternalSchema schema) {
    InternalField sourceField =
        findField(schema, key)
            .orElseThrow(() -> new ReadException("Partition key not found in schema: " + key));
    return InternalPartitionField.builder()
        .sourceField(sourceField)
        .transformType(PartitionTransformType.VALUE)
        .build();
  }

  private Optional<InternalField> findField(InternalSchema schema, String path) {
    return schema.getAllFields().stream().filter(f -> f.getPath().equals(path)).findFirst();
  }

  private InternalRowPartitionComputer newPartitionComputer(FileStoreTable table) {
    return new InternalRowPartitionComputer(
        table.coreOptions().partitionDefaultName(),
        table.store().partitionType(),
        table.partitionKeys().toArray(new String[0]),
        table.coreOptions().legacyPartitionName());
  }
}
