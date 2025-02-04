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
 
package org.apache.xtable.hms;

import static org.apache.xtable.catalog.CatalogUtils.toHierarchicalTableIdentifier;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.xtable.catalog.CatalogTableBuilder;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.hms.table.IcebergHMSCatalogTableBuilder;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.catalog.HierarchicalTableIdentifier;
import org.apache.xtable.model.storage.TableFormat;

public class HMSCatalogTableBuilderFactory {

  public static CatalogTableBuilder<Table, Table> getTableBuilder(
      String tableFormat, Configuration configuration) {
    switch (tableFormat) {
      case TableFormat.ICEBERG:
        return new IcebergHMSCatalogTableBuilder(configuration);
      default:
        throw new NotSupportedException("Unsupported table format: " + tableFormat);
    }
  }

  public static Table newHmsTable(
      CatalogTableIdentifier tblIdentifier,
      StorageDescriptor storageDescriptor,
      Map<String, String> params) {
    HierarchicalTableIdentifier tableIdentifier = toHierarchicalTableIdentifier(tblIdentifier);
    try {
      Table newTb = new Table();
      newTb.setDbName(tableIdentifier.getDatabaseName());
      newTb.setTableName(tableIdentifier.getTableName());
      newTb.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
      newTb.setCreateTime((int) Instant.now().getEpochSecond());
      newTb.setSd(storageDescriptor);
      newTb.setTableType(TableType.EXTERNAL_TABLE.toString());
      newTb.setParameters(params);
      return newTb;
    } catch (IOException e) {
      throw new RuntimeException("Failed to set owner for hms table: " + tableIdentifier, e);
    }
  }
}
