# Running a Local Demo
This demo was created for the 2023 Open Source Data Summit. It shows how OneTable can be used with two existing datasets.

Use `./start_demo.sh` to spin up a local notebook with a scala interpreter, Hive Metastore, and Trino in docker containers. The script will first build the OneTable jars required for the demo and then start the containers.  

## Accessing Services
### Jupyter Notebook
To access the notebook, look for a log line during startup that contains `To access the server, open this file in a browser: ...  Or copy and paste one of these URLs: ...` and use one of those to open the notebook in your browser. The demo is located at `work/demo.ipynb`. 
### Trino
You can access the local Trino container by running `docker exec -it trino trino`