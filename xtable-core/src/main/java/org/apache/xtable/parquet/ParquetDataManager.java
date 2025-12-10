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

import java.io.IOException;
import java.time.Instant;

import lombok.Builder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

@Builder
public class ParquetDataManager {
  private static final String TIME_FIELD = "modified_at";
  public Path outputFile;
  private ParquetReader<Group> reader;
  private ParquetMetadataExtractor metadataExtractor = ParquetMetadataExtractor.getInstance();

  private Path writeNewParquetFile(Configuration conf, ParquetReader reader, Path file) {
    ParquetFileConfig parquetFileConfig = new ParquetFileConfig(conf, file);
    int pageSize = ParquetWriter.DEFAULT_PAGE_SIZE;
    try (ParquetWriter<Group> writer =
        new ParquetWriter<Group>(
            outputFile,
            new GroupWriteSupport(),
            parquetFileConfig.getCodec(),
            (int) parquetFileConfig.getRowGroupSize(),
            pageSize,
            pageSize, // dictionaryPageSize
            true, // enableDictionary
            false, // enableValidation
            ParquetWriter.DEFAULT_WRITER_VERSION,
            conf)) {
      Group currentGroup = null;
      while ((currentGroup = (Group) reader.read()) != null) {
        writer.write(currentGroup);
      }
    } catch (Exception e) {

      e.printStackTrace();
    }
    return outputFile;
  }

  public long convertTimeField(Type timeFieldType, int timeIndex, Group record) {
    PrimitiveType timeFieldTypePrimitive = timeFieldType.asPrimitiveType();
    long timestampValue = record.getLong(timeIndex, 0);

    // Check the logical type to determine the unit:
    LogicalTypeAnnotation logicalType = timeFieldTypePrimitive.getLogicalTypeAnnotation();
    // check the cases of Nanos and millis as well
    if (logicalType != null
        && logicalType.equals(
            LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))) {
      // convert microseconds to milliseconds
      timestampValue = timestampValue / 1000;
    }
    // continue case INT96 and BINARY (time as string)
    return timestampValue;
  }

  public ParquetReader readParquetDataGroupsFromTime(
      String filePath, Configuration conf, Instant modifTime) {
    Path file = new Path(filePath);
    try {
      reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();
      MessageType schema =
          metadataExtractor.readParquetMetadata(conf, file).getFileMetaData().getSchema();
      Group record = null;

      // get the index of the modified_at field
      int timeIndex = schema.getFieldIndex(TIME_FIELD);
      Type timeFieldType = null;
      long recordTime = 0L;
      int isAfterModificationTime = 0;
      while ((reader.read()) != null) {
        // TODO check if TIME_FIELD needs to be extracted or can use default one
        timeFieldType = schema.getType(TIME_FIELD);
        recordTime = convertTimeField(timeFieldType, timeIndex, record);
        isAfterModificationTime = Instant.ofEpochMilli(recordTime).compareTo(modifTime);
        if (isAfterModificationTime >= 0) {
          break;
        }
      }
      // write the remaining reader using a writer

    } catch (IOException e) {
      e.printStackTrace();
    }

    return reader;
  }
}

class ParquetFileConfig {
  private final MessageType schema;
  private final long rowGroupSize;
  private final CompressionCodecName codec;

  public ParquetFileConfig(Configuration conf, Path file) {
    ParquetMetadata metadata =
        ParquetMetadataExtractor.getInstance().readParquetMetadata(conf, file);
    MessageType schema = metadata.getFileMetaData().getSchema();
    if (metadata.getBlocks().isEmpty()) {
      throw new IllegalStateException("Baseline Parquet file has no row groups.");
    }
    long rowGroupSize = metadata.getBlocks().get(0).getTotalByteSize();
    CompressionCodecName codec = metadata.getBlocks().get(0).getColumns().get(0).getCodec();
    this.schema = schema;
    this.rowGroupSize = rowGroupSize;
    this.codec = codec;
  }

  public MessageType getSchema() {
    return schema;
  }

  public long getRowGroupSize() {
    return rowGroupSize;
  }

  public CompressionCodecName getCodec() {
    return codec;
  }
}
