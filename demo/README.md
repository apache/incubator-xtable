<!--
 - Licensed to the Apache Software Foundation (ASF) under one
 - or more contributor license agreements.  See the NOTICE file
 - distributed with this work for additional information
 - regarding copyright ownership.  The ASF licenses this file
 - to you under the Apache License, Version 2.0 (the
 - "License"); you may not use this file except in compliance
 - with the License.  You may obtain a copy of the License at
 - 
 -     http://www.apache.org/licenses/LICENSE-2.0
 - 
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and 
 - limitations under the License.
-->

# Running a Local Demo
This demo was created for the 2023 Open Source Data Summit. It shows how XTable can be used with two existing datasets.

Use `./start_demo.sh` to spin up a local notebook with a scala interpreter, Hive Metastore, Presto and Trino in docker containers (in the background). The script builds the XTable jars required for the demo first (if they are not already present) and then starts the containers and prints the notebook URL. The jars can also be (re)built explicitly with `./build_demo.sh`, for example after making code changes.

When you are done, use `./stop_demo.sh` to stop and remove the containers. Pass `--reset-data` to also restore the seed datasets under `demo/data` to their original state so the demo can be re-run from scratch.

## Notebooks
- `work/demo.ipynb` — the main demo: two existing datasets (Hudi and Delta) are synced across Hudi, Delta and Iceberg, updated from Trino, joined, and validated from Trino.
- `work/hms_sync_demo.ipynb` — follows the [how-to](https://xtable.apache.org/docs/how-to) and [Hive Metastore](https://xtable.apache.org/docs/hms) guides: creates a Hudi table, syncs it to Delta and Iceberg with XTable, registers all three in the Hive Metastore, and queries them from Trino and Presto.

## Accessing Services
### Jupyter Notebook
To access the notebook, look for a log line during startup that contains `To access the server, open this file in a browser: ...  Or copy and paste one of these URLs: ...` and use the `http://127.0.0.1:8888/...` url to open the notebook in your browser. The demo is located at `work/demo.ipynb`. 
### Trino
You can access the local Trino container by running `docker exec -it trino trino`
### Presto
You can access the local Presto container by running `docker exec -it presto presto-cli --server localhost:8082`