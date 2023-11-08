---
sidebar_position: 1
title: "Features and Limitations"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Features and Limitations
## Features
OneTable provides users with the ability to translate metadata from one table format to another.  

OneTable provides two sync modes, "incremental" and "full." The incremental mode is more lightweight and has better performance, especially on large tables. If there is anything that prevents the incremental mode from working properly, the tool will fall back to the full sync mode.   
This sync provides users with the following: the following information in sync between the source and target tables:  
1. Syncing of data files along with their column level statistics and partition metadata 
2. Schema updates in the source are reflected in the target table metadata
3. Metadata maintenance for the target table formats. For Hudi, unreferenced files will be marked as cleaned to control the size of the metadata table. For Iceberg, snapshots will be expired after a configured amount of time. For Delta, the transaction log will be retained for a configured amount of time.

## Limitations and Compatibility Notes
### General
- Only Copy-on-Write or Read-Optimized views of tables are currently supported. This means that only the underlying parquet files are synced but log files from Hudi and delete vectors from Delta and Iceberg are not captured by the sync.

### Hudi
- Hudi 0.14.0 is required when reading a Hudi target table. Users will also need to enable the metadata table (`hoodie.metadata.enable=true`) when reading the data.
- Be sure to enable `parquet.avro.write-old-list-structure=false` for proper compatibility with lists when syncing from Hudi to Iceberg.
- When using Hudi as the source for an Iceberg target, you may require field IDs set in the parquet schema. To enable that, follow the instructions [here](https://github.com/onetable-io/onetable/tree/main/hudi-support/extensions).

### Delta
- When using Delta as the source for an Iceberg target, you may require field IDs set in the parquet schema. To enable that, follow the instructions for enabling column mapping [here](https://docs.delta.io/latest/delta-column-mapping.html).
