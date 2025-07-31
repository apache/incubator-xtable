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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import lombok.NonNull;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import org.apache.xtable.hudi.PathBasedPartitionValuesExtractor;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;

/** Partition value extractor for Parquet. */
// Extracts the partitionFields and values and create InputParitionFields object (fields and types)
// then convert those to InternalPartitionField for the ConversionSource
// @NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ParquetPartitionValueExtractor extends PathBasedPartitionValuesExtractor {
  private static final ParquetPartitionValueExtractor INSTANCE =
      new ParquetPartitionValueExtractor(Collections.emptyMap());
  private static final ParquetSchemaExtractor schemaExtractor =
      ParquetSchemaExtractor.getInstance();
  private static final ParquetMetadataExtractor parquetMetadataExtractor =
      ParquetMetadataExtractor.getInstance();

    private static final ParquetPartitionSpecExtractor partitionsSpecExtractor =
        ParquetPartitionSpecExtractor.getInstance();
  //private ParquetPartitionSpecExtractor partitionsSpecExtractor;

  public ParquetPartitionValueExtractor(@NonNull Map<String, String> pathToPartitionFieldFormat) {
    super(pathToPartitionFieldFormat);
  }

  public static ParquetPartitionValueExtractor getInstance() {
    return INSTANCE;
  }

  public List<InternalPartitionField> extractParquetPartitions(
      ParquetMetadata footer, String path) {
    MessageType parquetSchema = parquetMetadataExtractor.getSchema(footer);
    InternalSchema internalSchema = schemaExtractor.toInternalSchema(parquetSchema, path);
    List<InternalPartitionField> partitions = partitionsSpecExtractor.spec(internalSchema);
    return partitions;
  }
}
