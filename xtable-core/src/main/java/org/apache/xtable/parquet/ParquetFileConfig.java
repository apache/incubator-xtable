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

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Builder
class ParquetFileConfig {
  MessageType schema;
  ParquetMetadata metadata;
  long rowGroupIndex;
  long modifTime;
  long size;
  CompressionCodecName codec;
  Path path;

  public ParquetFileConfig(Configuration conf, Path file) {
    long modifTime = -1L;
    ParquetMetadata metadata =
        ParquetMetadataExtractor.getInstance().readParquetMetadata(conf, file);

    if (metadata.getBlocks().isEmpty()) {
      throw new IllegalStateException("Parquet file contains no row groups.");
    }
    try {
      modifTime = file.getFileSystem(conf).getFileStatus(file).getModificationTime();
    } catch (IOException e) {
      e.printStackTrace();
    }
    this.path = file;
    this.modifTime = modifTime;
    this.size = metadata.getBlocks().stream().mapToLong(BlockMetaData::getTotalByteSize).sum();
    this.metadata = metadata;
    this.schema = metadata.getFileMetaData().getSchema();
    this.rowGroupIndex = metadata.getBlocks().size();
    this.codec = metadata.getBlocks().get(0).getColumns().get(0).getCodec();
  }
}
