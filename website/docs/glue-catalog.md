---
sidebar_position: 2
title: "Glue Data Catalog"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Syncing to Glue Data Catalog
This document walks through the steps to register an Apache XTable™ (Incubating) synced table in Glue Data Catalog on AWS.

## Pre-requisites
1. Source table(s) (Hudi/Delta/Iceberg) already written to Amazon S3.
   If you don't have the source table written in S3 already,
   you can follow the steps in [this](/docs/how-to#create-dataset) tutorial to set it up
2. Setup access to interact with AWS APIs from the command line.
   If you haven’t installed AWSCLIv2, you do so by following the steps outlined in
   [AWS docs](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) and
   also set up access credentials by following the steps
   [here](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html)
3. Clone the Apache XTable™ (Incubating) [repository](https://github.com/apache/incubator-xtable) and create the
   `utilities-0.1.0-SNAPSHOT-bundled.jar` by following the steps on the [Installation page](/docs/setup)

## Steps
### Running sync
Create `my_config.yaml` in the cloned Apache XTable™ (Incubating) directory.

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

```yaml md title="yaml"
sourceFormat: DELTA|ICEBERG # choose only one
targetFormats:
  - HUDI
datasets:
  -
    tableBasePath: s3://path/to/source/data
    tableName: table_name
```

</TabItem>
<TabItem value="delta">

```yaml md title="yaml"
sourceFormat: HUDI|ICEBERG # choose only one
targetFormats:
  - DELTA
datasets:
  -
    tableBasePath: s3://path/to/source/data
    tableName: table_name
    partitionSpec: partitionpath:VALUE # you only need to specify partitionSpec for HUDI sourceFormat
```

</TabItem>
<TabItem value="iceberg">

```yaml md title="yaml"
sourceFormat: HUDI|DELTA # choose only one
targetFormats:
  - ICEBERG
datasets:
  -
    tableBasePath: s3://path/to/source/data
    tableName: table_name
    partitionSpec: partitionpath:VALUE # you only need to specify partitionSpec for HUDI sourceFormat
```

</TabItem>
</Tabs>

:::note Note:
Replace with appropriate values for `sourceFormat`, `tableBasePath` and `tableName` fields.
:::

From your terminal under the cloned xtable directory, run the sync process using the below command.

 ```shell md title="shell"
 java -jar utilities/target/utilities-0.1.0-SNAPSHOT-bundled.jar --datasetConfig my_config.yaml
 ```

:::tip Note:
At this point, if you check your bucket path, you will be able to see the `.hoodie` or `_delta_log` or `metadata` directory
with metadata files which contains the information that helps query engines interpret the data as the target table.
:::

### Register the target table in Glue Data Catalog
From your terminal, create a glue database.
   
 ```shell md title="shell"
 aws glue create-database --database-input "{\"Name\":\"xtable_synced_db\"}"
 ```

From your terminal, create a glue crawler. Modify the `<yourAccountId>`, `<yourRoleName>` 
and `<path/to/your/data>`, with appropriate values.

```shell md title="shell"
export accountId=<yourAccountId>
export roleName=<yourRoleName>
export s3DataPath=s3://<path/to/source/data>
```

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

```shell md title="shell"
aws glue create-crawler --name xtable_crawler --role arn:aws:iam::${accountId}:role/service-role/${roleName} --database xtable_synced_db --targets "{\"HudiTargets\":[{\"Paths\":[\"${s3DataPath}\"]}]}"
```

</TabItem>
<TabItem value="delta">

```shell md title="shell"
aws glue create-crawler --name xtable_crawler --role arn:aws:iam::${accountId}:role/service-role/${roleName} --database xtable_synced_db --targets "{\"DeltaTargets\":[{\"Paths\":[\"${s3DataPath}\"]}]}"
```

</TabItem>
<TabItem value="iceberg">

```shell md title="shell"
aws glue create-crawler --name xtable_crawler --role arn:aws:iam::${accountId}:role/service-role/${roleName} --database xtable_synced_db --targets "{\"IcebergTargets\":[{\"Paths\":[\"${s3DataPath}\"]}]}"
```

</TabItem>
</Tabs>

From your terminal, run the glue crawler.

```shell md title="shell"
 aws glue start-crawler --name xtable_crawler
```
Once the crawler succeeds, you’ll be able to query this Iceberg table from Athena,
EMR and/or Redshift query engines.

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

:::danger LIMITATION for Hudi target format:
To validate the Hudi targetFormat table results, you need to ensure that the query engine that you're using
supports Hudi version 0.14.0 as mentioned [here](/docs/features-and-limitations#hudi)
:::

</TabItem>
<TabItem value="delta">

### Validating the results
After the crawler runs successfully, you can inspect the catalogued tables in Glue
and also query the table in Amazon Athena like below:

```sql
SELECT * FROM xtable_synced_db.<table_name>;
```

</TabItem>
<TabItem value="iceberg">

### Validating the results
After the crawler runs successfully, you can inspect the catalogued tables in Glue
and also query the table in Amazon Athena like below:

```sql
SELECT * FROM xtable_synced_db.<table_name>;
```

</TabItem>
</Tabs>

## Conclusion
In this guide we saw how to,
1. sync a source table to create metadata for the desired target table formats using Apache XTable™ (Incubating)
2. catalog the data in the target table format in Glue Data Catalog
3. query the target table using Amazon Athena
