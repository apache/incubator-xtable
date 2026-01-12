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

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * Manages Parquet file operations including reading, writing, and partition discovery and path
 * construction.
 *
 * <p>This class provides functions to handle Parquet metadata, validate schemas during appends, and
 * calculate target partition directories based on file modification times and defined partition
 * fields.
 */
@Log4j2
public class ParquetDataManager {
  public static final ParquetDataManager INSTANCE = new ParquetDataManager();

  public static ParquetDataManager getInstance() {
    return INSTANCE;
  }

  public List<ParquetFileConfig> getParquetFilesMetadataInRange(
      Configuration conf, Stream<LocatedFileStatus> parquetFiles, long startTime, long endTime) {

    return parquetFiles
        .filter(
            file ->
                file.getModificationTime() >= startTime && file.getModificationTime() <= endTime)
        .map(file -> new ParquetFileConfig(conf, file.getPath()))
        .collect(Collectors.toList());
  }

  public List<ParquetFileConfig> getParquetFilesMetadataAfterTime(
      Configuration conf, Stream<LocatedFileStatus> parquetFiles, long syncTime) {

    return parquetFiles
        .filter(file -> file.getModificationTime() >= syncTime)
        .map(file -> new ParquetFileConfig(conf, file.getPath()))
        .collect(Collectors.toList());
  }
}
