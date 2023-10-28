---
sidebar_position: 3
---

# Apache Spark
To read a Onetable synced target table (regardless of the table format) in Apache Spark locally or on services like
Amazon EMR or Google's Cloud Data Proc, you do not need additional jars or configs other than what is needed by the
respective table formats.

Refer to the project specific documentation for the required configurations that needs to be passed in when
you create the spark session or when you submit a spark job.

* For Hudi, refer the [Spark Guide](https://hudi.apache.org/docs/quick-start-guide#spark-shellsql) page
* For Delta Lake, refer the [Set up interactive shell](https://docs.delta.io/latest/quick-start.html#set-up-interactive-shell) page
* For Iceberg, refer Using [Iceberg in Spark 3](https://iceberg.apache.org/docs/latest/getting-started/#using-iceberg-in-spark-3) page

```python md title="pyspark-hudi"
df = spark.read.format("hudi").load("/path/to/hudi_table")
```
```python md title="pyspark-delta"
df = spark.read.format("delta").load("/path/to/delta_table")
```
```python md title="pyspark-iceberg"
df = spark.read.format("iceberg").load("/path/to/iceberg_table")
```
