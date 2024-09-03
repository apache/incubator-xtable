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

Use `./start_demo.sh` to spin up a local notebook with a scala interpreter, Hive Metastore, Presto and Trino in docker containers. The script will first build the XTable jars required for the demo and then start the containers.  

## Accessing Services
### Jupyter Notebook
To access the notebook, look for a log line during startup that contains `To access the server, open this file in a browser: ...  Or copy and paste one of these URLs: ...` and use the `http://127.0.0.1:8888/...` url to open the notebook in your browser. The demo is located at `work/demo.ipynb`. 
### Trino
You can access the local Trino container by running `docker exec -it trino trino`
### Presto
You can access the local Presto container by running `docker exec -it presto presto-cli --server localhost:8082`