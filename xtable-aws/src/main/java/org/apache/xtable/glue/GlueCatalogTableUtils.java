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
 
package org.apache.xtable.glue;

import static org.apache.xtable.catalog.CatalogUtils.toHierarchicalTableIdentifier;

import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.catalog.HierarchicalTableIdentifier;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.Table;

/** Utils class to fetch details about Glue table */
public class GlueCatalogTableUtils {

  /**
   * Retrieves a table from AWS Glue based on the provided catalog and table identifiers.
   *
   * @param glueClient The AWS Glue client used to connect to AWS Glue.
   * @param catalogId The ID of the AWS Glue Data Catalog where the table is located.
   * @param catalogTableIdentifier The identifier for the table in the catalog.
   * @return The retrieved {@link Table} object if found; otherwise, returns {@code null} if the
   *     table does not exist.
   */
  public static Table getTable(
      GlueClient glueClient, String catalogId, CatalogTableIdentifier catalogTableIdentifier) {
    HierarchicalTableIdentifier tableIdentifier =
        toHierarchicalTableIdentifier(catalogTableIdentifier);
    GetTableResponse response =
        glueClient.getTable(
            GetTableRequest.builder()
                .catalogId(catalogId)
                .databaseName(tableIdentifier.getDatabaseName())
                .name(tableIdentifier.getTableName())
                .build());
    return response.table();
  }
}
