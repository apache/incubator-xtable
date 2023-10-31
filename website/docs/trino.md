---
sidebar_position: 7
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Trino

Trino just like Presto allows you to query table formats like Hudi, Delta and Iceberg tables using connectors.
Users do not need additional configurations to work with Onetable synced tables.

For more information and required configurations refer to:
* [Hudi Connector](https://trino.io/docs/current/connector/hudi.html)
* [Delta Lake Connector](https://trino.io/docs/current/connector/delta-lake.html)
* [Iceberg Connector](https://trino.io/docs/current/connector/iceberg.html)

For hands on experimentation, please follow [Creating your first interoperable table](https://link/to/how/to/create/dataset)
to create Onetable synced tables followed by [Hive Metastore](https://link/to/hms) to register the target table
in Hive Metastore. Once done, please follow the below high level steps:
1. Start the Trino server manually if you are working with a non-managed Trino service:
   from the trino-server directory run `./bin/launcher run`
2. From the directory where you have installed trino-cli: login to trino-cli by running `./trino-cli`
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
If you are following the example from [Hive Metastore](https://link/to/hms), you can query the Onetable synced hudi table
from Trino using the below query.
```sql md title="sql"
SELECT * FROM hudi.hudi_db.table_name;
```
:::

</TabItem>
<TabItem value="delta">

:::tip Note
If you are following the example from [Hive Metastore](https://link/to/hms), you can query the Onetable synced delta table
from Trino using the below query.
```sql md title="sql"
SELECT * FROM delta.delta_db.table_name;
```
:::

</TabItem>
<TabItem value="iceberg">

:::tip Note
If you are following the example from [Hive Metastore](https://link/to/hms), you can query the Onetable synced iceberg table
from Trino using the below query.
```sql md title="sql"
SELECT * FROM iceberg.iceberg_db.table_name;
```
:::

</TabItem>
</Tabs>