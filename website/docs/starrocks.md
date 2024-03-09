---
sidebar_position: 8
title: "StarRocks"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Querying from StarRocks

StarRocks allows you to query table formats like Hudi, Delta and Iceberg tables using our [external catalog](https://docs.starrocks.io/docs/data_source/catalog/catalog_overview/) feature.
Users do not need additional configurations to work with Apache XTable™ (Incubating) synced tables.

For more information and required configurations refer to:
* [Hudi Catalog](https://docs.starrocks.io/docs/data_source/catalog/hudi_catalog/)
* [Delta Lake Catalog](https://docs.starrocks.io/docs/data_source/catalog/deltalake_catalog/)
* [Iceberg Catalog](https://docs.starrocks.io/docs/data_source/catalog/iceberg_catalog/)

For hands on experimentation, please follow [Creating your first interoperable table](/docs/how-to#create-dataset)
to create Apache XTable™ (Incubating) synced tables followed by [Hive Metastore](/docs/hms) to register the target table
in Hive Metastore. Once done, please follow the below high level steps:
1. Start the StarRocks server 
2. From the directory where you have installed mysql-cli: login by running `mysql -P 9030 -h 127.0.0.1 -u root --prompt="StarRocks > "`.

<Tabs
groupId="table-format"
defaultValue="hudi"
values={[
{ label: 'targetFormat: HUDI', value: 'hudi', },
{ label: 'targetFormat: DELTA', value: 'delta', },
{ label: 'targetFormat: ICEBERG', value: 'iceberg', },
]}
>
<TabItem value="hudi">

:::tip Note:
If you are following the example from [Hive Metastore](/docs/hms), you can query the Apache XTable™ (Incubating) synced Hudi table
from StarRocks using the below query.
```sql md title="sql"
CREATE EXTERNAL CATALOG unified_catalog_hms PROPERTIES ("type" = "unified","unified.metastore.type" = "hive", "hive.metastore.uris" = "thrift://hivemetastore:9083" );
SELECT * FROM unified_catalog_hms.hudi_db.<table_name>;
```
:::

</TabItem>
<TabItem value="delta">

:::tip Note:
If you are following the example from [Hive Metastore](/docs/hms), you can query the Apache XTable™ (Incubating) synced Delta table
from StarRocks using the below query.
```sql md title="sql"
CREATE EXTERNAL CATALOG unified_catalog_hms PROPERTIES ("type" = "unified","unified.metastore.type" = "hive", "hive.metastore.uris" = "thrift://hivemetastore:9083" );
SELECT * FROM unified_catalog_hms.delta_db.<table_name>;
```
:::

</TabItem>
<TabItem value="iceberg">

:::tip Note:
If you are following the example from [Hive Metastore](/docs/hms), you can query the Apache XTable™ (Incubating) synced Iceberg table
from StarRocks using the below query.
```sql md title="sql"
CREATE EXTERNAL CATALOG unified_catalog_hms PROPERTIES ("type" = "unified","unified.metastore.type" = "hive", "hive.metastore.uris" = "thrift://hivemetastore:9083" );
SELECT * FROM unified_catalog_hms.iceberg_db.<table_name>;
```
:::

</TabItem>
</Tabs>
