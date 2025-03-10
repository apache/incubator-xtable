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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.expressions.Expressions;

/** Syncs Partition spec for Iceberg. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IcebergPartitionSpecSync {
  private static final IcebergPartitionSpecSync INSTANCE = new IcebergPartitionSpecSync();

  public static IcebergPartitionSpecSync getInstance() {
    return INSTANCE;
  }

  public void sync(PartitionSpec current, PartitionSpec latest, Transaction transaction) {
    if (!current.fields().equals(latest.fields())) {
      // Partition evolution
      UpdatePartitionSpec updateSpec = transaction.updateSpec();
      Set<String> currentFieldNames =
          current.fields().stream().map(PartitionField::name).collect(Collectors.toSet());
      Set<String> latestFieldNames =
          latest.fields().stream().map(PartitionField::name).collect(Collectors.toSet());
      List<PartitionField> partitionFieldsToRemove =
          current.fields().stream()
              .filter(field -> !latestFieldNames.contains(field.name()))
              .collect(Collectors.toList());
      List<PartitionField> partitionFieldsToAdd =
          latest.fields().stream()
              .filter(field -> !currentFieldNames.contains(field.name()))
              .collect(Collectors.toList());
      for (PartitionField partitionFieldToRemove : partitionFieldsToRemove) {
        updateSpec.removeField(partitionFieldToRemove.name());
      }
      for (PartitionField partitionFieldToAdd : partitionFieldsToAdd) {
        String fieldName =
            transaction.table().schema().findField(partitionFieldToAdd.sourceId()).name();
        updateSpec.addField(Expressions.transform(fieldName, partitionFieldToAdd.transform()));
      }
      updateSpec.commit();
    }
  }
}
