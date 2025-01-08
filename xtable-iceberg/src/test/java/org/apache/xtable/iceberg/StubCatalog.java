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
 
package org.apache.xtable.iceberg;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

public class StubCatalog implements Catalog {
  // since the class is instantiated with reflection, we need a way to pass in the mocks
  private static final Map<String, Catalog> REGISTERED_MOCKS = new HashMap<>();

  public static void registerMock(String catalogName, Catalog catalog) {
    REGISTERED_MOCKS.put(catalogName, catalog);
  }
  // use a mocked catalog instance to more easily test
  private Catalog mockedCatalog;

  public StubCatalog() {}

  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.mockedCatalog = REGISTERED_MOCKS.get(name);
    mockedCatalog.initialize(name, properties);
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return null;
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    return false;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {}

  @Override
  public boolean tableExists(TableIdentifier identifier) {
    return mockedCatalog.tableExists(identifier);
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {
    return mockedCatalog.createTable(identifier, schema, spec, location, properties);
  }

  @Override
  public Table loadTable(TableIdentifier identifier) {
    return mockedCatalog.loadTable(identifier);
  }
}
