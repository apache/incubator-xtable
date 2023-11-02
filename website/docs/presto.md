---
sidebar_position: 5
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Presto

Presto allows you to query table formats like Hudi, Delta and Iceberg using connectors. The same setup will
work for Onetable synced tables as well.

For more information and required configurations refer to:
* [Hudi Connector](https://prestodb.io/docs/current/connector/hudi.html)
* [Delta Lake](https://prestodb.io/docs/current/connector/deltalake.html)
* [Iceberg Connector](https://prestodb.io/docs/current/connector/iceberg.html)

:::danger Delta Lake:
Currently Onetable generated partition columns i.e. TIMESTAMP (or DATE) based partitions will show `NULL` when
queried from Presto CLI. Partition columns with STRING type will not have any issues.
:::

For hands on experimentation, please follow [Creating your first interoperable table](https://onetable.dev/docs/setup) tutorial
to create Onetable synced tables followed by [Hive Metastore](https://onetable.dev/docs/hms) tutorial to register the target table
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

:::tip Note 
If you are following the example from [Hive Metastore](https://onetable.dev/docs/hms), you can query the Onetable synced Hudi table 
from Presto using the below query.
```sql md title="sql"
SELECT * FROM hudi.hudi_db.<table_name>;
```
:::

</TabItem>
<TabItem value="delta">

:::tip Note
If you are following the example from [Hive Metastore](https://onetable.dev/docs/hms), you can query the Onetable synced Delta table
from Presto using the below query.
```sql md title="sql"
SELECT * FROM delta.delta_db.<table_name>;
```
:::

</TabItem>
<TabItem value="iceberg">

:::tip Note
If you are following the example from [Hive Metastore](https://onetable.dev/docs/hms), you can query the Onetable synced Iceberg table
from Presto using the below query.
```sql md title="sql"
SELECT * FROM iceberg.iceberg_db.<table_name>;
```
:::

</TabItem>
</Tabs>
