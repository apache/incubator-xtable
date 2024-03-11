---
sidebar_position: 3
title: "Apache Spark"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Querying from Apache Spark
To read an Apache XTable™ (Incubating) synced target table (regardless of the table format) in Apache Spark locally or on services like
Amazon EMR, Google Cloud's Dataproc, Azure HDInsight, or Databricks, you do not need additional jars or configs 
other than what is needed by the respective table formats.

Refer to the project specific documentation for the required configurations that needs to be passed in when
you create the spark session or when you submit a spark job.

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

* For Hudi, refer the [Spark Guide](https://hudi.apache.org/docs/quick-start-guide#spark-shellsql) page

:::danger LIMITATION for Hudi target format:
To validate the Hudi targetFormat table results, you need to ensure that you're using Hudi version 0.14.0 as mentioned [here](/docs/features-and-limitations#hudi)
:::

```python md title="python"

hudi_options = {
    "hoodie.metadata.enable": "true",
    "hoodie.datasource.write.hive_style_partitioning": "true",
}

df = spark.read.format("hudi").options(**hudi_options).load("/path/to/source/data")
```

</TabItem>
<TabItem value="delta">

* For Delta Lake, refer the [Set up interactive shell](https://docs.delta.io/latest/quick-start.html#set-up-interactive-shell) page

```python md title="python"
df = spark.read.format("delta").load("/path/to/source/data")
```

</TabItem>
<TabItem value="iceberg">

* For Iceberg, refer Using [Iceberg in Spark 3](https://iceberg.apache.org/docs/latest/getting-started/#using-iceberg-in-spark-3) page

```python md title="python"
df = spark.read.format("iceberg").load("/path/to/source/data")
```

</TabItem>
</Tabs>