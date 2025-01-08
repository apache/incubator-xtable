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

import java.util.Collections;
import java.util.List;

import lombok.Builder;

import org.apache.iceberg.DataFile;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;

/** Extractor of data files for Iceberg */
@Builder
public class IcebergDataFileExtractor {

  @Builder.Default
  private IcebergPartitionValueConverter partitionValueConverter =
      IcebergPartitionValueConverter.getInstance();

  /**
   * Builds {@link InternalDataFile} representation from Iceberg {@link DataFile}.
   *
   * @param dataFile Iceberg data file
   * @param partitionValues representation of partition fields and ranges
   * @param schema current schema for the table, used for mapping field IDs to stats
   * @return corresponding InternalTable data file
   */
  InternalDataFile fromIceberg(
      DataFile dataFile, List<PartitionValue> partitionValues, InternalSchema schema) {
    return fromIceberg(dataFile, partitionValues, schema, true);
  }

  private InternalDataFile fromIceberg(
      DataFile dataFile,
      List<PartitionValue> partitionValues,
      InternalSchema schema,
      boolean includeColumnStats) {
    List<ColumnStat> columnStats =
        includeColumnStats
            ? IcebergColumnStatsConverter.getInstance()
                .fromIceberg(
                    schema.getAllFields(),
                    dataFile.valueCounts(),
                    dataFile.nullValueCounts(),
                    dataFile.columnSizes(),
                    dataFile.lowerBounds(),
                    dataFile.upperBounds())
            : Collections.emptyList();
    String filePath = dataFile.path().toString();
    // assume path without scheme is local file
    if (!filePath.contains(":")) {
      filePath = "file:" + filePath;
    }
    return InternalDataFile.builder()
        .physicalPath(filePath)
        .fileFormat(fromIcebergFileFormat(dataFile.format()))
        .fileSizeBytes(dataFile.fileSizeInBytes())
        .recordCount(dataFile.recordCount())
        .partitionValues(partitionValues)
        .columnStats(columnStats)
        .build();
  }

  /**
   * Maps Iceberg file format to InternalTable file format
   *
   * @param format Iceberg file format
   * @return corresponding InternalTable file format
   */
  FileFormat fromIcebergFileFormat(org.apache.iceberg.FileFormat format) {
    switch (format) {
      case PARQUET:
        return FileFormat.APACHE_PARQUET;
      case ORC:
        return FileFormat.APACHE_ORC;
      case AVRO:
        return FileFormat.APACHE_AVRO;
      default:
        throw new NotSupportedException("Unsupported file format: " + format);
    }
  }
}
