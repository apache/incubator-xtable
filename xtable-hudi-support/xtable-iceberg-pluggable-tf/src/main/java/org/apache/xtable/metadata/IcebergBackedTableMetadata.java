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
 
package org.apache.xtable.metadata;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.storage.StoragePathInfo;

/**
 * File listing for a table under the Iceberg table format. Iceberg is the source of truth for
 * readers, so this is plain storage listing on the Hudi side, restricted to the files Hudi wrote.
 */
public class IcebergBackedTableMetadata extends FileSystemBackedTableMetadata {
  public IcebergBackedTableMetadata(
      HoodieEngineContext engineContext, HoodieStorage storage, String datasetBasePath) {
    super(engineContext, storage, datasetBasePath);
  }

  /**
   * The Iceberg table may hold data files Hudi did not write (written before the table was adopted,
   * or by a native Iceberg writer). They belong to Iceberg snapshots, not to Hudi file groups, and
   * Hudi cannot parse a file id or commit time out of their names, so keep them out of the listing
   * the file-system view is built from.
   */
  static boolean isHudiFile(StoragePathInfo pathInfo) {
    StoragePath path = pathInfo.getPath();
    if (!FSUtils.isDataFile(path) || FSUtils.isLogFile(path)) {
      return true;
    }
    try {
      return FSUtils.getCommitTime(path.getName()) != null;
    } catch (RuntimeException e) {
      return false;
    }
  }

  private static List<StoragePathInfo> hudiFiles(List<StoragePathInfo> files) {
    return files.stream()
        .filter(IcebergBackedTableMetadata::isHudiFile)
        .collect(Collectors.toList());
  }

  @Override
  public List<StoragePathInfo> getAllFilesInPartition(StoragePath partitionPath)
      throws IOException {
    return hudiFiles(super.getAllFilesInPartition(partitionPath));
  }

  @Override
  public Map<String, List<StoragePathInfo>> getAllFilesInPartitions(
      Collection<String> partitionPaths, Option<StoragePathFilter> pathFilter) throws IOException {
    Map<String, List<StoragePathInfo>> result = new HashMap<>();
    super.getAllFilesInPartitions(partitionPaths, pathFilter)
        .forEach((partition, files) -> result.put(partition, hudiFiles(files)));
    return result;
  }

  @Override
  public Map<Pair<String, StoragePath>, List<StoragePathInfo>> listPartitions(
      List<Pair<String, StoragePath>> partitionPaths) throws IOException {
    Map<Pair<String, StoragePath>, List<StoragePathInfo>> result = new HashMap<>();
    super.listPartitions(partitionPaths)
        .forEach((partition, files) -> result.put(partition, hudiFiles(files)));
    return result;
  }
}
