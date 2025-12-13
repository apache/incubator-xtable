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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import lombok.Builder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;

@Builder
public class ParquetDataManager {
  private ParquetMetadataExtractor metadataExtractor = ParquetMetadataExtractor.getInstance();

  public ParquetReader readParquetDataAsReader(String filePath, Configuration conf) {
    ParquetReader reader = null;
    Path file = new Path(filePath);
    try {
      reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return reader;
  }

  private Path appendNewParquetFile(
      Configuration conf,
      String rootPath,
      Path fileToAppend,
      String partitionColumn,
      Instant modifTime) {
    Path finalFile = null;
    // construct the file path to inject into the existing partitioned file
    String partitionValue =
        DateTimeFormatter.ISO_LOCAL_DATE.format(
            modifTime.atZone(ZoneId.systemDefault()).toLocalDate());
    String partitionDir = partitionColumn + partitionValue;
    String fileName = "part-" + System.currentTimeMillis() + "-" + UUID.randomUUID() + ".parquet";
    Path outputFile = new Path(new Path(rootPath, partitionDir), fileName);
    // return its reader for convenience of writing
    ParquetReader reader = readParquetDataAsReader(fileToAppend.getName(), conf);
    // append/write it in the right partition
    finalFile = writeNewParquetFile(conf, reader, fileToAppend, outputFile);
    return finalFile;
  }

  private ParquetFileConfig getParquetFileConfig(Configuration conf, Path fileToAppend) {
    ParquetFileConfig parquetFileConfig = new ParquetFileConfig(conf, fileToAppend);
    return parquetFileConfig;
  }

  private Path writeNewParquetFile(
      Configuration conf, ParquetReader reader, Path fileToAppend, Path outputFile) {
    ParquetFileConfig parquetFileConfig = getParquetFileConfig(conf, fileToAppend);
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
}
