<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# RFC-[1]: XCatalogSync - Synchronize tables across catalogs

## Proposers

- @vinishjail97

## Approvers

- Anyone from XTable community can approve/add feedback.

## Status

GH Feature Request: https://github.com/apache/incubator-xtable/issues/590

> Please keep the status updated in `rfc/README.md`.

## Abstract

Users of Apache XTable (Incubating) today can translate metadata across table formats (iceberg, hudi, and delta) and use the tables in different platforms depending on their choice. 
Today there's still some friction involved in terms of usability because users need to explicitly [register](https://xtable.apache.org/docs/catalogs-index) the tables in the catalog of their choice (glue, HMS, unity, bigLake etc.) 
and then use the catalog in the platform of their choice to do DDL, DML queries.

## Background
XTable is built on the principle of omnidirectional interoperability, and I'm proposing an interface which allows syncing metadata of table formats to multiple catalogs in a continuous and incremental manner. With this new functionality we will be able to      
1. Reduce friction for XTable users - XTable sync will register the tables in the catalogs of their choice after metadata generation. If users are using a single format, they can still use XTable to sync the metadata across multiple catalogs.
2. Avoid catalog lock-in - There's no reason why data/metadata in storage should be registered in a single catalog, users can register the table across multiple catalogs depending on the use-case, ecosystem and features provided by the catalog.

## Implementation

Introducing the following interfaces. [[PR]]( https://github.com/apache/incubator-xtable/pull/603)
1. `CatalogSyncClient`: This interface contains methods that are responsible for creating table, refreshing table metadata, dropping table etc. in target catalog. Consider this interface as a translation layer between InternalTable and the catalog's table object. 
2. `CatalogSync`: This interface synchronizes the internal XTable object (InternalTable) to multiple target catalogs using the methods available in `CatalogSyncClient` interface.
3. `CatalogTableIdentifier`: Represents a catalog table identifier in a multi-level catalog system. `HierarchicalTableIdentifier` is an internal representation of a fully qualified table identifier within a catalog following the three level hierarchy convention (it's used by all the major catalogs glue, hms, unity etc.). In the future, we can support other conventions by implementing this interface.

For XTable users, defining their source/target catalog configurations and synchronizing tables will be handled by the `RunCatalogSync` class. This utility class parses the user’s YAML configuration, synchronizes table format metadata when necessary, and then uses the previously defined interfaces to synchronize the table in the catalog.
[[PR]]( https://github.com/apache/incubator-xtable/pull/591)

User's YAML configuration.
1. `sourceCatalog`: Configuration of the source catalog from which XTable will read. It must contain all the necessary connection and access details for describing and listing tables.
    1. `catalogId`:  A user-defined unique identifier for the catalog, allows user to sync table to multiple catalogs of the same name/type eg: HMS catalog with url1, HMS catalog with url2.
    2. `catalogType`: The type of the source catalog. This might be a specific type understood by XTable, such as Hive, Glue etc.
    3. `catalogSyncClientImpl`(optional): A fully qualified class name that implements the interface for `CatalogSyncClient`, it can be used if the implementation for catalogType doesn't exist in XTable.
    4. `catalogConversionSourceImpl`(optional): A fully qualified class name that implements the interface for `CatalogConversionSource`, it can be used if the implementation for catalogType doesn't exist in XTable.
    5. `catalogProperties`: A collection of configs used to configure access or connection properties for the catalog.
2. `targetCatalogs`: Defines configuration one or more target catalogs, to which XTable will write or update tables. Unlike the source, these catalogs must be writable.
3. `datasets`: A list of datasets that specify how a source table maps to one or more target tables.
   1. `sourceCatalogTableIdentifier`: Identifies the source table in sourceCatalog. This can be done in two ways:
      1. `tableIdentifier`: Specifies a source table by its 3 level hierarchical fully qualified name - catalogName, databaseName and tableName. If catalogName is not provided, the default catalog will be used.     
      2. `storageIdentifier`(optional): Provides direct storage details such as a table’s base path (like an S3 location) and the partition specification. This allows reading from a source even if it is not strictly registered in a catalog, as long as the format and location are known
   2. `targetCatalogTableIdentifiers`: A list of one or more targets that this source table should be written to.
      1. `catalogId`: The user defined unique identifier of the target catalog where the table will be created or updated. The targetCatalog's id passed here should be one of the `targetCatalogs` defined above.
      2. `tableFormat`: The target table format (e.g., DELTA, HUDI, ICEBERG), specifying how the data will be stored at the target.
      3. `tableIdentifier`: Specifies a target table by its 3 level hierarchical fully qualified name - catalogName, databaseName and tableName. If catalogName is not provided, the default catalog will be used.
```
sourceCatalog:
  catalogName: "source-1"
  catalogType: "catalog-type-1"
  catalogConversionSourceImpl: "org.apache.xtable.utilities.CustomCatalogConversionSourceImpl"
  catalogProperties:
    key01: "value01"
    key02: "value02"
    key03: "value03"
targetCatalogs:
  - catalogName: "target-1"
    catalogType: "catalog-type-2"
    catalogProperties:
      key11: "value11"
      key12: "value22"
      key13: "value33"
  - catalogName: "target-2"
    catalogSyncClientImpl: "org.apache.xtable.utilities.CustomCatalogSyncClientImpl"
    catalogProperties:
      key21: "value21"
      key22: "value22"
      key23: "value23"
datasets:
  - sourceCatalogTableIdentifier:
      tableIdentifier:
        hierarchicalId: "source-database-1.source-1"
    targetCatalogTableIdentifiers:
      - catalogName: "target-1"
        tableFormat: "DELTA"
        tableIdentifier:
          hierarchicalId: "target-database-1.target-tableName-1"
      - catalogName: "target-1"
        tableFormat: "ICEBERG"
        tableIdentifier:
          hierarchicalId: "target-database-2.target-tableName-2-iceberg"
      - catalogName: "target-2"
        tableFormat: "HUDI"
        tableIdentifier:
          hierarchicalId: "default-catalog-2.target-database-2.target-tableName-2-hudi"
  - sourceCatalogTableIdentifier:
      storageIdentifier:
        tableBasePath: s3://tpc-ds-datasets/1GB/hudi/catalog_sales
        tableName: catalog_sales
        partitionSpec: cs_sold_date_sk:VALUE
        tableFormat: "HUDI"
    targetCatalogTableIdentifiers:
      - catalogName: "target-2"
        tableFormat: "ICEBERG"
        tableIdentifier:
          hierarchicalId: "target-database-2.target-tableName-2"
```

## Overview of the `RunCatalogSync` process
![img.jpg](assets/images/catalog_sync_flow.jpg)


## Rollout/Adoption Plan

- Are there any breaking changes as part of this new feature/functionality?
  - In `SyncResult` status has been refactored to `tableFormatSyncStatus` for clarity.
- What impact (if any) will there be on existing users? 
  - No on existing users, this is a new functionality being added to synchronize tables across catalogs. Existing XTable users can still use the table format sync in `RunSync` without any issues.
- If we are changing behavior how will we phase out the older behavior? When will we remove the existing behavior? 
  - N/A
- If we need special migration tools, describe them here.
  - N/A

## Test Plan

We plan to add the HMS and Glue implementations for `CatalogSyncClient` interface, conversion in both ways across all table formats will be tested.