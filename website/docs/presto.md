---
sidebar_position: 5
---

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

For hands on experimentation, please follow [Creating your first interoperable table](https://link/to/how/to) to create
Onetable synced tables. Once done, please follow the below high level steps:
1. Start the Presto server manually if you are working with a non-managed Presto service `./bin/launcher run`
2. Login to presto-cli `./presto-cli`
3. Start querying the tables i.e. `SELECT * FROM hudi.onetable_synced_schema.trips_data;`
