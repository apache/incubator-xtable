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

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.TableMetadataFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

/**
 * Serves the Hudi metadata table when the writer has one, as the native format does, and the file
 * system listing otherwise. Iceberg manifests are not read for metadata yet, so a writer that
 * enables the metadata table gets the same index lookups it would on a native table.
 */
public class IcebergMetadataFactory extends TableMetadataFactory {
  private static final IcebergMetadataFactory INSTANCE = new IcebergMetadataFactory();

  public static IcebergMetadataFactory getInstance() {
    return INSTANCE;
  }

  @Override
  public HoodieTableMetadata create(
      HoodieEngineContext engineContext,
      HoodieStorage storage,
      HoodieMetadataConfig metadataConfig,
      String datasetBasePath,
      boolean reuse) {
    // The metadata table builds file paths from the base path it is given, while a file system
    // listing returns qualified paths; the hooks compare the two forms, so both are qualified.
    String qualifiedBasePath =
        new StoragePath(datasetBasePath).makeQualified(storage.getUri()).toString();
    if (metadataConfig.isEnabled()) {
      HoodieBackedTableMetadata metadata =
          new HoodieBackedTableMetadata(
              engineContext, storage, metadataConfig, qualifiedBasePath, reuse);
      if (metadata.isMetadataTableInitialized()) {
        return metadata;
      }
    }
    return new IcebergBackedTableMetadata(engineContext, storage, qualifiedBasePath);
  }
}
