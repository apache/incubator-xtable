---
sidebar_position: 1
title: "Features and Limitations"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Features and Limitations
## Features

### Synchronizing table format metadata (TableFormatSync)
Apache XTable™ (Incubating) provides users with the ability to translate metadata from one table format to another.  

Apache XTable™ (Incubating) provides two sync modes, "incremental" and "full." The incremental mode is more lightweight and has better performance, especially on large tables. If there is anything that prevents the incremental mode from working properly, the tool will fall back to the full sync mode.

This sync provides users with the following:   
1. Syncing of data files along with their column level statistics and partition metadata 
2. Schema updates in the source are reflected in the target table metadata
3. Metadata maintenance for the target table formats.
   * For Hudi, unreferenced files will be marked as [cleaned](https://hudi.apache.org/docs/hoodie_cleaner/) to control the size of the metadata table.
   * For Iceberg, snapshots will be [expired](https://iceberg.apache.org/docs/latest/maintenance/#expire-snapshots) after a configured amount of time.
   * For Delta, the transaction log will be [retained](https://docs.databricks.com/en/sql/language-manual/delta-vacuum.html) for a configured amount of time.

### Synchronizing table format metadata in external catalogs (CatalogSync)
In addition to synchronizing table format metadata, Apache XTable™ (Incubating) now allows users to synchronize metadata for tables across multiple external catalogs continuously and incrementally.
This reduces friction by eliminating the manual step of registering tables in multiple catalogs and enhances flexibility by avoiding catalog lock-in.
HMS and AWS Glue are the two catalogs supported right now, support for other catalogs (Unity, Apache Polaris, Apache Gravitino, DataHub) coming soon. 

### Secondary index for Iceberg tables
A sync to a Hudi target can build a secondary index on a column of an Iceberg source table and keep it current on each later sync.
The index is stored as a Hudi metadata table in the data folder of the table, and Spark sessions with the XTable Spark SQL extensions use it to read only the files that hold the values of a point lookup.
See [Secondary index for Iceberg tables](/docs/how-to-index).

### Writing Hudi tables as Iceberg tables
The `xtable-iceberg-pluggable-tf` module plugs Iceberg into Hudi's pluggable table format support, so a Hudi writer commits an Iceberg snapshot with each Hudi commit and the table reads as an Iceberg table without a sync job.
See [Write Hudi tables as Iceberg tables](/docs/how-to-iceberg-table-format).


## Limitations and Compatibility Notes
### General
- Only Copy-on-Write or Read-Optimized views of tables are currently supported. This means that only the underlying parquet files are synced but log files from Hudi and [delete vectors](https://docs.delta.io/latest/delta-deletion-vectors.html#:~:text=Deletion%20vectors%20indicate%20changes%20to,is%20run%20on%20the%20table.) from Delta and Iceberg are not captured by the sync.

### Hudi
- XTable uses Hudi 1.2.1. As a source, it reads Hudi tables of table version 6 (written by Hudi 0.14.x and 0.15.x), 8 (Hudi 1.0.x) and 9 (Hudi 1.1.x and 1.2.x) without upgrading them.
- As a target, XTable writes table version 6 by default. Set the target property `xtable.hudi.target.table_version` to `9` to write the Hudi 1.x timeline and the version 2 column statistics index instead. Table versions 6 and 9 are the only supported values.
- Hudi 0.14.0 or later is required when reading a table version 6 Hudi target table, and Hudi 1.1.0 or later for a table version 9 target. Users will also need to enable 
  - the metadata table (`hoodie.metadata.enable=true`) and 
  - hive style partitioning (`hoodie.datasource.write.hive_style_partitioning=true`) wherever applicable when reading the data.
- Hudi targets accept these target properties, which are set per table under `targetProperties.HUDI` in the dataset config of `RunSync` and `XTableSparkSync`:

  | Property | Default | Meaning |
  |---|---|---|
  | `xtable.hudi.target.table_version` | `6` | Hudi table version to write, `6` or `9` |
  | `xtable.hudi.target.execution.engine` | `java` | Engine that writes the Hudi commit and the metadata table indexes, `java` or `spark` |
  | `xtable.hudi.target.secondary.index.column` | | Column to build a [secondary index](/docs/how-to-index) on. Needs table version `9` and the `spark` engine |
- Be sure to enable `parquet.avro.write-old-list-structure=false` for proper compatibility with lists when syncing from Hudi to Iceberg.
- When using Hudi as the source for an Iceberg target, you may require field IDs set in the parquet schema. To enable that, follow the instructions [here](https://github.com/apache/incubator-xtable/tree/main/xtable-hudi-support/xtable-hudi-support-extensions).

### Delta
- When using Delta as the source for an Iceberg target, you may require field IDs set in the parquet schema. To enable that, follow the instructions for enabling column mapping [here](https://docs.delta.io/latest/delta-column-mapping.html).
- When Delta is the source, Generated Columns are not synced to the target schema. For tables that are partitioned on Generated Columns, there is limited support. For example, we support date functions like transforming a timestamp to `yyyy-MM-dd` format. Please file a GitHub issue or pull-request for any cases that you think should be supported.

### Parquet
- Schema evolution across files in a Parquet source directory is not supported. XTable derives the table schema from the footer of a file with the most recent filesystem modification time; it does not merge or comprehensively validate schemas across all files. If the files have different schemas, synchronization may use a schema that does not represent every file or may fail while processing them. Copying or restoring files can also change their modification times and therefore change which schema XTable selects.
- Hive-style partition columns that exist only in directory paths are not supported. When partition extraction is configured, XTable resolves each source field against the selected file's schema. If a partition column exists only in a directory path, such as `region=eu/`, XTable cannot synthesize the field or infer its type, and synchronization fails.
