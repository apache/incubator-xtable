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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import lombok.NonNull;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import org.apache.xtable.exception.PartitionValuesExtractorException;
import org.apache.xtable.hudi.PathBasedPartitionValuesExtractor;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;

/** Partition value extractor for Parquet. */
public class ParquetPartitionValueExtractor extends PathBasedPartitionValuesExtractor {
  private static final ParquetPartitionValueExtractor INSTANCE =
      new ParquetPartitionValueExtractor(Collections.emptyMap());
  private static final ParquetSchemaExtractor schemaExtractor =
      ParquetSchemaExtractor.getInstance();
  private static final ParquetMetadataExtractor parquetMetadataExtractor =
      ParquetMetadataExtractor.getInstance();

  public ParquetPartitionValueExtractor(@NonNull Map<String, String> pathToPartitionFieldFormat) {
    super(pathToPartitionFieldFormat);
  }

  public List<PartitionValue> extractPartitionValues(
          List<InternalPartitionField> partitionColumns, String partitionPath) {
    if (partitionColumns == null) {
      return Collections.emptyList();
    }
    int totalNumberOfPartitions = partitionColumns.size();
    List<PartitionValue> result = new ArrayList<>(totalNumberOfPartitions);
    String remainingPartitionPath = partitionPath;
    for (InternalPartitionField partitionField : partitionColumns) {
      for (String partitionFieldName : partitionField.getPartitionFieldNames()) {
      if (remainingPartitionPath.startsWith(partitionFieldName + "=")) {
        remainingPartitionPath = remainingPartitionPath.substring(partitionFieldName.length() + 1);
      }
      PartialResult valueAndRemainingPath =
              parsePartitionPath(partitionField, remainingPartitionPath, totalNumberOfPartitions);

      result.add(
              PartitionValue.builder()
                      .partitionField(partitionField)
                      .range(Range.scalar(valueAndRemainingPath.getValue()))
                      .build());
      remainingPartitionPath = valueAndRemainingPath.getRemainingPath();
    }}
    return result;
  }
  protected PartialResult parsePartitionPath(
          InternalPartitionField field, String remainingPath, int totalNumberOfPartitions) {
    switch (field.getTransformType()) {
      case YEAR:
        return parseDate(
                remainingPath,
                pathToPartitionFieldFormat.get(
                        field.getSourceField().getName()).split("=")[1]);
      case MONTH://TODO split and get the value of month from pathToPartitionFieldFormat
      case DAY://TODO split and get the value of day from pathToPartitionFieldFormat
      case HOUR:
        return parseDate(
                remainingPath,
                pathToPartitionFieldFormat.get(
                        field.getSourceField().getName())); // changed from getPath() TODO split and get the value of hour from pathToPartitionFieldFormat
      case VALUE:
        // if there is only one partition field, then assume full partition path is used even if
        // value contains slashes
        // this case is possible if user is directly relying on directly _hoodie_partition_path due
        // to custom partitioning logic
        boolean isSlashDelimited = totalNumberOfPartitions > 1;
        return parseValue(
                remainingPath, field.getSourceField().getSchema().getDataType(), isSlashDelimited);
      default:
        throw new IllegalArgumentException(
                "Unexpected partition type: " + field.getTransformType());
    }
  }
  public static ParquetPartitionValueExtractor getInstance() {
    return INSTANCE;
  }

  public InternalSchema extractSchemaForParquetPartitions(ParquetMetadata footer, String path) {
    MessageType parquetSchema = parquetMetadataExtractor.getSchema(footer);
    return schemaExtractor.toInternalSchema(parquetSchema, path);
  }
}
