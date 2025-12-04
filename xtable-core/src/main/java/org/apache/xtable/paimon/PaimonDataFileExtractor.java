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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.SnapshotReader;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.InternalDataFile;

public class PaimonDataFileExtractor {

  private final PaimonPartitionExtractor partitionExtractor =
      PaimonPartitionExtractor.getInstance();

  private static final PaimonDataFileExtractor INSTANCE = new PaimonDataFileExtractor();

  public static PaimonDataFileExtractor getInstance() {
    return INSTANCE;
  }

  public List<InternalDataFile> toInternalDataFiles(
      FileStoreTable table, Snapshot snapshot, InternalSchema internalSchema) {
    List<InternalDataFile> result = new ArrayList<>();
    Iterator<ManifestEntry> manifestEntryIterator =
        newSnapshotReader(table, snapshot).readFileIterator();
    while (manifestEntryIterator.hasNext()) {
      result.add(toInternalDataFile(table, manifestEntryIterator.next(), internalSchema));
    }
    return result;
  }

  private InternalDataFile toInternalDataFile(
      FileStoreTable table, ManifestEntry entry, InternalSchema internalSchema) {
    return InternalDataFile.builder()
        .physicalPath(toFullPhysicalPath(table, entry))
        .fileSizeBytes(entry.file().fileSize())
        .lastModified(entry.file().creationTimeEpochMillis())
        .recordCount(entry.file().rowCount())
        .partitionValues(
            partitionExtractor.toPartitionValues(table, entry.partition(), internalSchema))
        .columnStats(toColumnStats(entry.file(), internalSchema))
        .build();
  }

  private String toFullPhysicalPath(FileStoreTable table, ManifestEntry entry) {
    String basePath = table.location().toString();
    String bucketPath = "bucket-" + entry.bucket();
    String filePath = entry.file().fileName();

    Optional<String> partitionPath = partitionExtractor.toPartitionPath(table, entry.partition());
    if (partitionPath.isPresent()) {
      return String.join("/", basePath, partitionPath.get(), bucketPath, filePath);
    } else {
      return String.join("/", basePath, bucketPath, filePath);
    }
  }

  private List<ColumnStat> toColumnStats(DataFileMeta file, InternalSchema internalSchema) {
    SimpleStats stats = file.valueStats();
    if (stats == null) {
      return Collections.emptyList();
    }
    List<String> colNames = file.valueStatsCols();
    if (colNames == null || colNames.isEmpty()) {
      return Collections.emptyList();
    }

    Map<String, InternalField> fieldMap =
        internalSchema.getAllFields().stream()
            .collect(Collectors.toMap(InternalField::getPath, f -> f));

    List<ColumnStat> columnStats = new ArrayList<>();
    BinaryRow minValues = stats.minValues();
    BinaryRow maxValues = stats.maxValues();
    BinaryArray nullCounts = stats.nullCounts();

    for (int i = 0; i < colNames.size(); i++) {
      String colName = colNames.get(i);
      InternalField field = fieldMap.get(colName);
      if (field == null) {
        continue;
      }

      InternalType type = field.getSchema().getDataType();
      Object min = getValue(minValues, i, type, field.getSchema());
      Object max = getValue(maxValues, i, type, field.getSchema());
      Long nullCount = nullCounts.getLong(i);

      columnStats.add(
          ColumnStat.builder()
              .field(field)
              .range(Range.vector(min, max))
              .numNulls(nullCount)
              .numValues(file.rowCount())
              .build());
    }
    return columnStats;
  }

  private Object getValue(
      BinaryRow row, int index, InternalType type, InternalSchema fieldSchema) {
    if (row.isNullAt(index)) {
      return null;
    }
    switch (type) {
      case BOOLEAN:
        return row.getBoolean(index);
      case INT:
      case DATE:
        return row.getInt(index);
      case LONG:
      case TIMESTAMP:
      case TIMESTAMP_NTZ:
        return row.getLong(index);
      case FLOAT:
        return row.getFloat(index);
      case DOUBLE:
        return row.getDouble(index);
      case STRING:
      case ENUM:
        return row.getString(index).toString();
      case DECIMAL:
        int precision =
            (int) fieldSchema.getMetadata().get(InternalSchema.MetadataKey.DECIMAL_PRECISION);
        int scale = (int) fieldSchema.getMetadata().get(InternalSchema.MetadataKey.DECIMAL_SCALE);
        return row.getDecimal(index, precision, scale).toBigDecimal();
      case FIXED:
      case BYTES:
        byte[] bytes = row.getBinary(index);
        return bytes != null ? ByteBuffer.wrap(bytes) : null;
      default:
        return null;
    }
  }

  private SnapshotReader newSnapshotReader(FileStoreTable table, Snapshot snapshot) {
    // If the table has primary keys, we read only the top level files
    // which means we can only consider fully compacted files.
    if (!table.schema().primaryKeys().isEmpty()) {
      return table
          .newSnapshotReader()
          .withLevel(table.coreOptions().numLevels() - 1)
          .withSnapshot(snapshot);
    } else {
      return table.newSnapshotReader().withSnapshot(snapshot);
    }
  }
}
