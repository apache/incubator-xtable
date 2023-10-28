---
sidebar_position: 7
---

# Trino

Trino just like Presto allows you to query table formats like Hudi, Delta and Iceberg tables using connectors.
Users do not need additional configurations to work with Onetable synced tables.

For more information and required configurations refer to:
* [Hudi Connector](https://trino.io/docs/current/connector/hudi.html)
* [Delta Lake Connector](https://trino.io/docs/current/connector/delta-lake.html)
* [Iceberg Connector](https://trino.io/docs/current/connector/iceberg.html)

For hands on experimentation, please follow [Creating your first interoperable table](https://link/to/how/to) to create
Onetable synced tables. Once done, please follow the below high level steps:
1. Start the Trino server manually if you are working with a non-managed Trino service `./bin/launcher run`
2. Login to trino-cli `./trino-cli`
3. Start querying the tables i.e. `SELECT * FROM hudi.onetable_synced_schema.trips_data;`