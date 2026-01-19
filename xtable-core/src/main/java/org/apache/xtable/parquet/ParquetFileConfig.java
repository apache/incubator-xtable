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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import org.apache.xtable.exception.ReadException;

@Log4j2
@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Builder
@AllArgsConstructor
class ParquetFileConfig {
  MessageType schema;
  ParquetMetadata metadata;
  long rowGroupIndex;
  long modificationTime;
  long size;
  CompressionCodecName codec;
  Path path;

  public ParquetFileConfig(Configuration conf, FileStatus file) {
    long modificationTime = -1L;
    ParquetMetadata metadata =
        ParquetMetadataExtractor.getInstance().readParquetMetadata(conf, file.getPath());

    if (metadata.getBlocks().isEmpty()) {
      throw new IllegalStateException("Parquet file contains no row groups.");
    }
    try {
      modificationTime = file.getModificationTime();
    } catch (ReadException e) {
      log.warn("File reading error: " + e.getMessage());
    }
    this.path = file.getPath();
    this.modificationTime = modificationTime;
    this.size = metadata.getBlocks().stream().mapToLong(BlockMetaData::getTotalByteSize).sum();
    this.metadata = metadata;
    this.schema = metadata.getFileMetaData().getSchema();
    this.rowGroupIndex = metadata.getBlocks().size();
    this.codec = metadata.getBlocks().get(0).getColumns().get(0).getCodec();
  }
}
