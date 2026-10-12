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
 
package org.apache.xtable.index;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;

/**
 * A catalog that keeps the current metadata location of each table in memory, like Glue or a Hive
 * Metastore keeps it in its own store. Its metadata files have UUID based names and there is no
 * version hint, so {@code HadoopTables} cannot load its tables from their location. The state is
 * static, because XTable creates its own catalog instance from the class name.
 */
public class TestMetastoreCatalog extends BaseMetastoreCatalog {
  private static final Map<String, String> METADATA_LOCATIONS = new ConcurrentHashMap<>();

  private String catalogName;
  private String warehouseLocation;
  private FileIO fileIO;

  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.catalogName = name;
    this.warehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
    this.fileIO = new HadoopFileIO(new Configuration());
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new TestMetastoreTableOperations(key(tableIdentifier), fileIO);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return warehouseLocation
        + "/"
        + String.join("/", tableIdentifier.namespace().levels())
        + "/"
        + tableIdentifier.name();
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    String prefix = catalogName + "." + namespace + ".";
    return METADATA_LOCATIONS.keySet().stream()
        .filter(key -> key.startsWith(prefix))
        .map(key -> TableIdentifier.of(namespace, key.substring(prefix.length())))
        .collect(Collectors.toList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    return METADATA_LOCATIONS.remove(key(identifier)) != null;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException("Renaming tables is not supported");
  }

  private String key(TableIdentifier tableIdentifier) {
    return catalogName + "." + tableIdentifier;
  }

  private static class TestMetastoreTableOperations extends BaseMetastoreTableOperations {
    private final String key;
    private final FileIO fileIO;

    TestMetastoreTableOperations(String key, FileIO fileIO) {
      this.key = key;
      this.fileIO = fileIO;
    }

    @Override
    protected String tableName() {
      return key;
    }

    @Override
    public FileIO io() {
      return fileIO;
    }

    @Override
    protected void doRefresh() {
      String metadataLocation = METADATA_LOCATIONS.get(key);
      if (metadataLocation == null) {
        disableRefresh();
      } else {
        refreshFromMetadataLocation(metadataLocation);
      }
    }

    @Override
    protected void doCommit(TableMetadata base, TableMetadata metadata) {
      String newMetadataLocation = writeNewMetadataIfRequired(base == null, metadata);
      String expectedLocation = base == null ? null : base.metadataFileLocation();
      METADATA_LOCATIONS.compute(
          key,
          (ignored, currentLocation) -> {
            if (!java.util.Objects.equals(currentLocation, expectedLocation)) {
              throw new CommitFailedException(
                  "Cannot commit %s: metadata location changed from %s to %s",
                  key, expectedLocation, currentLocation);
            }
            return newMetadataLocation;
          });
    }
  }
}
