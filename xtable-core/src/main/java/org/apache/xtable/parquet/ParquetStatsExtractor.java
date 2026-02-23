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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.Value;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import org.apache.xtable.hudi.PathBasedPartitionSpecExtractor;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.schema.SchemaFieldFinder;

@Value
@Builder
public class ParquetStatsExtractor {

  private static final ParquetStatsExtractor INSTANCE = new ParquetStatsExtractor();

  private static final ParquetSchemaExtractor schemaExtractor =
      ParquetSchemaExtractor.getInstance();

  private static final ParquetMetadataExtractor parquetMetadataExtractor =
      ParquetMetadataExtractor.getInstance();

  public static ParquetStatsExtractor getInstance() {
    return INSTANCE;
  }

  private static final ParquetPartitionValueExtractor partitionValueExtractor =
      ParquetPartitionValueExtractor.getInstance();
  private static PathBasedPartitionSpecExtractor partitionSpecExtractor =
      ParquetPartitionSpecExtractor.getInstance();

  @SuppressWarnings("unchecked")
  private static final Comparator<Object> COMPARABLE_COMPARATOR =
      (a, b) -> ((Comparable<Object>) a).compareTo(b);

  private static ColumnStat mergeColumnChunks(
      List<ColumnChunkMetaData> chunks, InternalSchema internalSchema) {
    ColumnChunkMetaData first = chunks.get(0);
    String dotStringPath = first.getPath().toDotString();
    InternalField internalField =
        SchemaFieldFinder.getInstance()
            .findFieldByPath(internalSchema, dotStringPath);
    Objects.requireNonNull(internalField, "No field found for path: " + dotStringPath);
    PrimitiveType primitiveType = first.getPrimitiveType();
    long totalNumValues = chunks.stream().mapToLong(ColumnChunkMetaData::getValueCount).sum();
    long totalSize = chunks.stream().mapToLong(ColumnChunkMetaData::getTotalSize).sum();
    long totalNullValues = chunks.stream().map(ColumnChunkMetaData::getStatistics).mapToLong(Statistics::getNumNulls).sum();
    Object globalMin =
        chunks.stream()
            .filter(c -> c.getStatistics().hasNonNullValue())
            .map(c -> convertStatsToInternalType(primitiveType, c.getStatistics().genericGetMin()))
            .min(COMPARABLE_COMPARATOR)
            .orElse(null);
    Object globalMax =
        chunks.stream()
            .filter(c -> c.getStatistics().hasNonNullValue())
            .map(c -> convertStatsToInternalType(primitiveType, c.getStatistics().genericGetMax()))
            .max(COMPARABLE_COMPARATOR)
            .orElse(null);
    return ColumnStat.builder()
        .field(internalField)
        .numValues(totalNumValues)
        .numNulls(totalNullValues)
        .totalSize(totalSize)
        .range(Range.vector(globalMin, globalMax))
        .build();
  }

  public static List<ColumnStat> getStatsForFile(
      ParquetMetadata footer, InternalSchema internalSchema) {
    MessageType schema = parquetMetadataExtractor.getSchema(footer);
    return footer.getBlocks().stream()
        .flatMap(block -> block.getColumns().stream())
        .collect(
            Collectors.groupingBy(chunk -> schema.getColumnDescription(chunk.getPath().toArray())))
        .values()
        .stream()
        .map(columnChunks -> mergeColumnChunks(columnChunks, internalSchema))
        .collect(Collectors.toList());
  }

  private static Object convertStatsToInternalType(PrimitiveType primitiveType, Object value) {
    LogicalTypeAnnotation annotation = primitiveType.getLogicalTypeAnnotation();

    // DECIMAL: convert unscaled backing value → BigDecimal regardless of primitive type
    if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
      int scale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) annotation).getScale();
      switch (primitiveType.getPrimitiveTypeName()) {
        case INT32:
        case INT64:
          return new BigDecimal(BigInteger.valueOf(((Number) value).longValue()), scale);
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          return new BigDecimal(new BigInteger(((Binary) value).getBytes()), scale);
        default:
          return value;
      }
    } else if (annotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
      // STRING: convert binary → String
      return new String(((Binary) value).getBytes(), StandardCharsets.UTF_8);
    }
    return value;
  }
}
