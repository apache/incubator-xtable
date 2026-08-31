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

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.storage.HoodieStorage;

/**
 * Serves Hudi's table metadata for a table using the Iceberg table format. It deliberately lists
 * the file system for now rather than reading Iceberg manifests, which is why the Hudi metadata
 * table has to stay disabled for such a table: the superclass throws on every index lookup, so an
 * enabled metadata table fails with "Unsupported operation: getColumnsStats".
 *
 * <p>The type exists to be replaced rather than removed. Iceberg manifests already carry the
 * per-column bounds and file listings this should eventually answer from, which is what RFC-93
 * means by the plugin's metadata serving the Hudi writer.
 */
public class IcebergBackedTableMetadata extends FileSystemBackedTableMetadata {

  public IcebergBackedTableMetadata(
      HoodieEngineContext engineContext, HoodieStorage storage, String datasetBasePath) {
    super(engineContext, storage, datasetBasePath);
  }
}
