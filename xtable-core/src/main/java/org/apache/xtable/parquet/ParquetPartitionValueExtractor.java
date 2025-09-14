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
  private static final ParquetPartitionSpecExtractor partitionSpecExtractor =
      ParquetPartitionSpecExtractor.getInstance();

  public ParquetPartitionValueExtractor(@NonNull Map<String, String> pathToPartitionFieldFormat) {
    super(pathToPartitionFieldFormat);
  }

  public List<PartitionValue> extractPartitionValues(
      List<InternalPartitionField> partitionColumns, String partitionPath) {
    PartialResult valueAndRemainingPath = null;
    String currentDateValue = "";
    List<String> parsedDateValues = new ArrayList<>();
    long dateSinceEpoch = 0L;
    if (partitionColumns.size() == 0) {
      return Collections.emptyList();
    }
    int totalNumberOfPartitions = partitionColumns.get(0).getPartitionFieldNames().size();
    List<PartitionValue> result = new ArrayList<>(totalNumberOfPartitions);
    String remainingPartitionPath = partitionPath;
    for (InternalPartitionField partitionField : partitionColumns) {
      int index = 0;
      for (String partitionFieldName : partitionField.getPartitionFieldNames()) {
        if (remainingPartitionPath.startsWith(partitionFieldName + "=")) {
          remainingPartitionPath =
              remainingPartitionPath.substring(partitionFieldName.length() + 1);
        }
        // parsePartitionPath shouldn't do two things but only gets the remaining path
        // to store the parsed value for the current partition field
        currentDateValue = parsePartitionDateValue(partitionField, remainingPartitionPath, index);
        parsedDateValues.add(currentDateValue);

        remainingPartitionPath =
            getRemainingPath(
                remainingPartitionPath,
                partitionSpecExtractor
                    .getListPartitionValuesFromFormatInput(
                        pathToPartitionFieldFormat.get(partitionField.getSourceField().getName()))
                    .get(index));
        index++;
      }
      try {
        result.add(
            PartitionValue.builder()
                .partitionField(partitionField)
                .range(
                    Range.scalar((Object) computeSinceEpochValue(parsedDateValues, partitionField)))
                .build());
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }
    return result;
  }

  protected String parsePartitionDateValue(
      InternalPartitionField field, String remainingPath, int index) {
    return parseDateValue(
        remainingPath,
        partitionSpecExtractor
            .getListPartitionValuesFromFormatInput(
                pathToPartitionFieldFormat.get(field.getSourceField().getName()))
            .get(index));
  }

  private long computeSinceEpochValue(
      List<String> parsedDateValues, InternalPartitionField partitionField) throws ParseException {
    // in the list of parsed values combine them using the standard way using hyphen symbol then
    // parse the date into sinceEpoch
    String combinedDate = String.join("-", parsedDateValues); // SimpleDateFormat works with hyphens
    // convert to since Epoch
    SimpleDateFormat simpleDateFormat =
        new SimpleDateFormat(
            String.join(
                "-",
                partitionSpecExtractor.getListPartitionValuesFromFormatInput(
                    pathToPartitionFieldFormat.get(partitionField.getSourceField().getName()))));
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    return simpleDateFormat.parse(combinedDate).toInstant().toEpochMilli();
  }

  protected static String parseDateValue(String remainingPath, String format) {
    return remainingPath.substring(0, format.length());
  }

  protected static String getRemainingPath(String remainingPath, String format) {
    return remainingPath.substring(Math.min(remainingPath.length(), format.length() + 1));
  }

  public static ParquetPartitionValueExtractor getInstance() {
    return INSTANCE;
  }

  public InternalSchema extractSchemaForParquetPartitions(ParquetMetadata footer, String path) {
    MessageType parquetSchema = parquetMetadataExtractor.getSchema(footer);
    return schemaExtractor.toInternalSchema(parquetSchema, path);
  }
}
