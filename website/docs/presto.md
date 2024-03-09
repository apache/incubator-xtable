---
sidebar_position: 6
title: "Presto"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Querying from Presto

Presto allows you to query table formats like Hudi, Delta and Iceberg using connectors. The same setup will
work for Apache XTable™ (Incubating) synced tables as well.

For more information and required configurations refer to:
* [Hudi Connector](https://prestodb.io/docs/current/connector/hudi.html)
* [Delta Lake](https://prestodb.io/docs/current/connector/deltalake.html)
* [Iceberg Connector](https://prestodb.io/docs/current/connector/iceberg.html)

:::danger Delta Lake:
Delta Lake supports [generated columns](https://docs.databricks.com/en/delta/generated-columns.html)
which are a special type of column whose values are automatically generated based on a user-specified function
over other columns in the Delta table. During sync, Apache XTable™ (Incubating) uses the same logic to generate partition columns wherever required. 
Currently, the generated columns from Apache XTable™ (Incubating) sync shows `NULL` when queried from Presto CLI.
:::

For hands on experimentation, please follow [Creating your first interoperable table](/docs/how-to) tutorial
to create Apache XTable™ (Incubating) synced tables followed by [Hive Metastore](/docs/hms) tutorial to register the target table
in Hive Metastore. Once done, follow the below high level steps:
1. If you are working with a self-managed Presto service, from the presto-server directory run `./bin/launcher run`
2. From the directory where you have installed presto-cli: login to presto-cli by running `./presto-cli`
3. Start querying the table i.e. `SELECT * FROM catalog.schema.table;`. 

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
from Presto using the below query.
```sql md title="sql"
SELECT * FROM hudi.hudi_db.<table_name>;
```
:::

</TabItem>
<TabItem value="delta">

:::tip Note:
If you are following the example from [Hive Metastore](/docs/hms), you can query the Apache XTable™ (Incubating) synced Delta table
from Presto using the below query.
```sql md title="sql"
SELECT * FROM delta.delta_db.<table_name>;
```
:::

</TabItem>
<TabItem value="iceberg">

:::tip Note:
If you are following the example from [Hive Metastore](/docs/hms), you can query the Apache XTable™ (Incubating) synced Iceberg table
from Presto using the below query.
```sql md title="sql"
SELECT * FROM iceberg.iceberg_db.<table_name>;
```
:::

</TabItem>
</Tabs>
