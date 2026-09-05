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
 
package org.apache.xtable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.exception.ReadException;

/**
 * Extracts positional deletes from the log files of a Hudi merge-on-read deltacommit so they can be
 * represented as Iceberg deletion vectors. Under the Iceberg table format, log files must only
 * carry delete blocks whose record positions reference the file slice's base file (the writer
 * guarantees this with hoodie.write.updates.as.deletes.and.inserts); a log data block or a delete
 * without positions cannot be represented and fails the sync.
 */
class HudiPositionalDeleteExtractor {

  private HudiPositionalDeleteExtractor() {}

  /** Returns the deleted row positions grouped by the full path of the base file they reference. */
  static Map<String, List<Long>> extractPositionalDeletes(
      HoodieCommitMetadata commitMetadata,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
    Map<String, List<Long>> positionsByDataFile = new HashMap<>();
    HoodieSchema schema = null;
    SyncableFileSystemView fsView = null;
    for (Map.Entry<String, List<HoodieWriteStat>> entry :
        commitMetadata.getPartitionToWriteStats().entrySet()) {
      String partitionPath = entry.getKey();
      for (HoodieWriteStat stat : entry.getValue()) {
        StoragePath relativePath = new StoragePath(stat.getPath());
        if (!FSUtils.isLogFile(relativePath)) {
          continue;
        }
        if (schema == null) {
          schema = readTableSchema(metaClient);
          fsView = viewManager.getFileSystemView(metaClient);
        }
        StoragePath fullPath = new StoragePath(metaClient.getBasePath(), stat.getPath());
        collectDeletesFromLogFile(
            new HoodieLogFile(fullPath),
            partitionPath,
            stat.getFileId(),
            metaClient,
            schema,
            fsView,
            positionsByDataFile);
      }
    }
    return positionsByDataFile;
  }

  private static void collectDeletesFromLogFile(
      HoodieLogFile logFile,
      String partitionPath,
      String fileId,
      HoodieTableMetaClient metaClient,
      HoodieSchema schema,
      SyncableFileSystemView fsView,
      Map<String, List<Long>> positionsByDataFile) {
    try (HoodieLogFormat.Reader reader =
        HoodieLogFormat.newReader(metaClient.getStorage(), logFile, schema)) {
      while (reader.hasNext()) {
        HoodieLogBlock block = reader.next();
        if (!(block instanceof HoodieDeleteBlock)) {
          throw new NotSupportedException(
              "The Iceberg table format only supports log files containing positional deletes, "
                  + "but found a "
                  + block.getClass().getSimpleName()
                  + " in "
                  + logFile.getPath());
        }
        HoodieDeleteBlock deleteBlock = (HoodieDeleteBlock) block;
        Roaring64NavigableMap positions = deleteBlock.getRecordPositions();
        int deleteCount = deleteBlock.getRecordsToDelete().length;
        if (positions == null || positions.getLongCardinality() != deleteCount) {
          throw new NotSupportedException(
              "Delete blocks must carry one valid record position per deleted record to be "
                  + "represented as an Iceberg deletion vector: "
                  + logFile.getPath());
        }
        String baseFileInstant = deleteBlock.getBaseFileInstantTimeOfPositions();
        String baseFilePath =
            resolveBaseFilePath(fsView, partitionPath, fileId, baseFileInstant, logFile);
        List<Long> collected =
            positionsByDataFile.computeIfAbsent(baseFilePath, ignored -> new ArrayList<>());
        positions.forEach(collected::add);
      }
    } catch (IOException e) {
      throw new ReadException("Failed to read the Hudi log file " + logFile.getPath(), e);
    }
  }

  private static String resolveBaseFilePath(
      SyncableFileSystemView fsView,
      String partitionPath,
      String fileId,
      String baseFileInstant,
      HoodieLogFile logFile) {
    if (baseFileInstant == null) {
      throw new NotSupportedException(
          "Delete blocks must reference the base file of their record positions: "
              + logFile.getPath());
    }
    Option<HoodieBaseFile> baseFile = fsView.getBaseFileOn(partitionPath, baseFileInstant, fileId);
    if (!baseFile.isPresent()) {
      throw new ReadException(
          String.format(
              "Cannot resolve the base file of file group %s at instant %s referenced by %s",
              fileId, baseFileInstant, logFile.getPath()));
    }
    return baseFile.get().getPath();
  }

  private static HoodieSchema readTableSchema(HoodieTableMetaClient metaClient) {
    try {
      return new TableSchemaResolver(metaClient).getTableSchema();
    } catch (Exception e) {
      throw new ReadException("Failed to resolve the Hudi table schema", e);
    }
  }
}
