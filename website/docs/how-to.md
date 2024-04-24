---
sidebar_position: 1
title: "Creating your first interoperable table"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Creating your first interoperable table

:::danger Important
Using Apache XTable™ (Incubating) to sync your source tables in different target format involves running sync on your 
current dataset using a bundled jar. You can create this bundled jar by following the instructions 
on the [Installation page](/docs/setup). Read through Apache XTable™'s 
[GitHub page](https://github.com/apache/incubator-xtable#building-the-project-and-running-tests) for more information.
:::

In this tutorial we will look at how to use Apache XTable™ (Incubating) to add interoperability between table formats. 
For example, you can expose a table ingested with Hudi as an Iceberg and/or Delta Lake table without
copying or moving the underlying data files used for that table while maintaining a similar commit 
history to enable proper point in time queries.

## Pre-requisites
1. A compute instance where you can run Apache Spark. This can be your local machine, docker,
   or a distributed service like Amazon EMR, Google Cloud's Dataproc, Azure HDInsight etc
2. Clone the Apache XTable™ (Incubating) [repository](https://github.com/apache/incubator-xtable) and create the
   `utilities-0.1.0-SNAPSHOT-bundled.jar` by following the steps on the [Installation page](/docs/setup)
3. Optional: Setup access to write to and/or read from distributed storage services like:
   * Amazon S3 by following the steps 
   [here](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) to install AWSCLIv2 
   and setup access credentials by following the steps
   [here](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html)
   * Google Cloud Storage by following the steps 
   [here](https://cloud.google.com/iam/docs/keys-create-delete#creating)

For the purpose of this tutorial, we will walk through the steps to using Apache XTable™ (Incubating) locally.

## Steps

### Initialize a pyspark shell
:::note Note:
You can choose to follow this example with `spark-sql` or `spark-shell` as well.
:::

<Tabs
groupId="table-format"
defaultValue="hudi"
values={[
{ label: 'Hudi', value: 'hudi', },
{ label: 'Delta', value: 'delta', },
{ label: 'Iceberg', value: 'iceberg', },
]}
>
<TabItem value="hudi">

```shell md title="shell"
pyspark \
  --packages org.apache.hudi:hudi-spark3.2-bundle_2.12:0.14.0 \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
  --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
```
</TabItem>
<TabItem value="delta">

```shell md title="shell"
pyspark \
  --packages io.delta:delta-core_2.12:2.1.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```
</TabItem>
<TabItem value="iceberg">

```shell md title="shell"
pyspark \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.4.1 \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog"
```
</TabItem>
</Tabs>

:::danger Note:
You may need additional configurations to write to external cloud storage locations like Amazon S3, GCS or ADLS
when you are working with spark locally. Refer to the respective cloud provider's documentation for more information.
:::

### Create dataset 
Write a source table locally.

<Tabs
groupId="table-format"
defaultValue="hudi"
values={[
{ label: 'Hudi', value: 'hudi', },
{ label: 'Delta', value: 'delta', },
{ label: 'Iceberg', value: 'iceberg', },
]}
>
<TabItem value="hudi">

```python md title="python"
from pyspark.sql.types import *

# initialize the bucket
table_name = "people"
local_base_path = "/tmp/hudi-dataset"

records = [
   (1, 'John', 25, 'NYC', '2023-09-28 00:00:00'),
   (2, 'Emily', 30, 'SFO', '2023-09-28 00:00:00'),
   (3, 'Michael', 35, 'ORD', '2023-09-28 00:00:00'),
   (4, 'Andrew', 40, 'NYC', '2023-10-28 00:00:00'),
   (5, 'Bob', 28, 'SEA', '2023-09-23 00:00:00'),
   (6, 'Charlie', 31, 'DFW', '2023-08-29 00:00:00')
]

schema = StructType([
   StructField("id", IntegerType(), True),
   StructField("name", StringType(), True),
   StructField("age", IntegerType(), True),
   StructField("city", StringType(), True),
   StructField("create_ts", StringType(), True)
])

df = spark.createDataFrame(records, schema)

hudi_options = {
   'hoodie.table.name': table_name,
   'hoodie.datasource.write.partitionpath.field': 'city',
   'hoodie.datasource.write.hive_style_partitioning': 'true'
}

(
   df.write
   .format("hudi")
   .options(**hudi_options)
   .save(f"{local_base_path}/{table_name}")
)
```
</TabItem>

<TabItem value="delta">

```python md title="python"
from pyspark.sql.types import *

# initialize the bucket
table_name = "people"
local_base_path = "/tmp/delta-dataset"

records = [
   (1, 'John', 25, 'NYC', '2023-09-28 00:00:00'),
   (2, 'Emily', 30, 'SFO', '2023-09-28 00:00:00'),
   (3, 'Michael', 35, 'ORD', '2023-09-28 00:00:00'),
   (4, 'Andrew', 40, 'NYC', '2023-10-28 00:00:00'),
   (5, 'Bob', 28, 'SEA', '2023-09-23 00:00:00'),
   (6, 'Charlie', 31, 'DFW', '2023-08-29 00:00:00')
]

schema = StructType([
   StructField("id", IntegerType(), True),
   StructField("name", StringType(), True),
   StructField("age", IntegerType(), True),
   StructField("city", StringType(), True),
   StructField("create_ts", StringType(), True)
])

df = spark.createDataFrame(records, schema)

(
   df.write
   .format("delta")
   .partitionBy("city")
   .save(f"{local_base_path}/{table_name}")
)
```
</TabItem>

<TabItem value="iceberg">

```python md title="python"
from pyspark.sql.types import *

# initialize the bucket
table_name = "people"
local_base_path = "/tmp/iceberg-dataset"

records = [
   (1, 'John', 25, 'NYC', '2023-09-28 00:00:00'),
   (2, 'Emily', 30, 'SFO', '2023-09-28 00:00:00'),
   (3, 'Michael', 35, 'ORD', '2023-09-28 00:00:00'),
   (4, 'Andrew', 40, 'NYC', '2023-10-28 00:00:00'),
   (5, 'Bob', 28, 'SEA', '2023-09-23 00:00:00'),
   (6, 'Charlie', 31, 'DFW', '2023-08-29 00:00:00')
]

schema = StructType([
   StructField("id", IntegerType(), True),
   StructField("name", StringType(), True),
   StructField("age", IntegerType(), True),
   StructField("city", StringType(), True),
   StructField("create_ts", StringType(), True)
])

df = spark.createDataFrame(records, schema)

(
   df.write
   .format("iceberg")
   .partitionBy("city")
   .save(f"{local_base_path}/{table_name}")
)
```
</TabItem>
</Tabs>


### Running sync 

Create `my_config.yaml` in the cloned xtable directory.

<Tabs
groupId="table-format"
defaultValue="hudi"
values={[
{ label: 'Hudi', value: 'hudi', },
{ label: 'Delta', value: 'delta', },
{ label: 'Iceberg', value: 'iceberg', },
]}
>

<TabItem value="hudi">

```yaml  md title="yaml"
sourceFormat: HUDI
targetFormats:
  - DELTA
  - ICEBERG
datasets:
  -
    tableBasePath: file:///tmp/hudi-dataset/people
    tableName: people
    partitionSpec: city:VALUE
```
</TabItem>

<TabItem value="delta">

```yaml  md title="yaml"
sourceFormat: DELTA
targetFormats:
  - HUDI
  - ICEBERG
datasets:
  -
    tableBasePath: file:///tmp/delta-dataset/people
    tableName: people
```
</TabItem>

<TabItem value="iceberg">

```yaml  md title="yaml"
sourceFormat: ICEBERG
targetFormats:
  - HUDI
  - DELTA
datasets:
  -
    tableBasePath: file:///tmp/iceberg-dataset/people
    tableDataPath: file:///tmp/iceberg-dataset/people/data
    tableName: people
```
:::note Note:
Add `tableDataPath` for ICEBERG sourceFormat if the `tableBasePath` is different from the path to the data.
:::

</TabItem>
</Tabs>

**Optional:** If your source table exists in Amazon S3, GCS or ADLS you should use a `yaml` file similar to below.

<Tabs
groupId="table-format"
defaultValue="hudi"
values={[
{ label: 'Hudi', value: 'hudi', },
{ label: 'Delta', value: 'delta', },
{ label: 'Iceberg', value: 'iceberg', },
]}
>
<TabItem value="hudi">

```yaml  md title="yaml"
sourceFormat: HUDI
targetFormats:
  - DELTA
  - ICEBERG
datasets:
  -
    tableBasePath: s3://path/to/hudi-dataset/people  # replace this with gs://path/to/hudi-dataset/people if your data is in GCS. 
    tableName: people
    partitionSpec: city:VALUE
```

</TabItem>
<TabItem value="delta">

```yaml  md title="yaml"
sourceFormat: DELTA
targetFormats:
  - HUDI
  - ICEBERG
datasets:
  -
    tableBasePath: s3://path/to/delta-dataset/people # replace this with gs://path/to/delta-dataset/people if your data is in GCS. 
    tableName: people
```

</TabItem>
<TabItem value="iceberg">

```yaml  md title="yaml"
sourceFormat: ICEBERG
targetFormats:
  - HUDI
  - DELTA
datasets:
  -
    tableBasePath: s3://path/to/iceberg-dataset/people  # replace this with gs://path/to/iceberg-dataset/people if your data is in GCS.
    tableDataPath: s3://path/to/iceberg-dataset/people/data
    tableName: people
```
:::note Note:
Add `tableDataPath` for ICEBERG sourceFormat if the `tableBasePath` is different from the path to the data.
:::

</TabItem>
</Tabs>

:::note Note:
Authentication for AWS is done with `com.amazonaws.auth.DefaultAWSCredentialsProviderChain`. 
To override this setting, specify a different implementation with the `--awsCredentialsProvider` option.

Authentication for GCP requires service account credentials to be exported. i.e.
`export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account_key.json`
:::

In your terminal under the cloned Apache XTable™ (Incubating) directory, run the below command.

```shell md title="shell"
java -jar utilities/target/utilities-0.1.0-SNAPSHOT-bundled.jar --datasetConfig my_config.yaml
```

**Optional:**
At this point, if you check your local path, you will be able to see the necessary metadata files that contain the schema, 
commit history, partitions, and column stats that helps query engines to interpret the data in the target table format.

## Conclusion
In this tutorial, we saw how to create a source table and use Apache XTable™ (Incubating) to create the metadata files 
that can be used to query the source table in different target table formats.

## Next steps
Go through the [Catalog Integration guides](/docs/catalogs-index) to register the Apache XTable™ (Incubating) synced tables
in different data catalogs.
