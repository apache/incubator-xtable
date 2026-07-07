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
import lombok.NoArgsConstructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

import org.apache.xtable.exception.ReadException;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ParquetMetadataExtractor {

  private static final ParquetMetadataExtractor INSTANCE = new ParquetMetadataExtractor();

  public static ParquetMetadataExtractor getInstance() {
    return INSTANCE;
  }

  public MessageType getSchema(ParquetMetadata footer) {
    return footer.getFileMetaData().getSchema();
  }

  public ParquetMetadata readParquetMetadata(Configuration conf, Path filePath) {
    ParquetReadOptions options = HadoopReadOptions.builder(conf, filePath).build();
    try (ParquetFileReader fileReader =
        ParquetFileReader.open(HadoopInputFile.fromPath(filePath, conf), options)) {
      return fileReader.getFooter();
    } catch (IOException e) {
      throw new ReadException("Failed to read the parquet file", e);
    }
  }
}
