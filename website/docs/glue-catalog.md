---
sidebar_position: 2
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Glue Data Catalog
This document walks through the steps to create a Onetable synced target table in Glue Data Catalog on AWS.

## Pre-requisites
1. Source table(s) (Hudi/Delta/Iceberg) already written to Amazon S3.
   If you don't have a Hudi table written in S3, you can follow the steps in [this](https://link-to-how-to/create-dataset.md)
   tutorial to set it up.
2. Setup access to interact with AWS APIs from the command line.
   If you haven’t installed AWSCLIv2, you do so by following the steps outlined in
   [AWS docs](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) and
   also set up access credentials by following the steps
   [here](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html).
3. Clone the onetable github [repository](https://github.com/onetable-io/onetable) and create the `utilities-0.1.0-SNAPSHOT-bundled.jar`
   by following the steps [here](https://github.com/onetable-io/onetable#building-the-project-and-running-tests).

## Steps

### Running sync
Create `my_config.yaml` in the cloned onetable directory.

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
    partitionSpec: partitionpath:VALUE
```
:::danger Note:
Add appropriate `sourceFormat`, `tableBasePath` and `tableName` values appropriately.
:::

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
    partitionSpec: partitionpath:VALUE
```
:::danger Note:
Add appropriate `sourceFormat`, `tableBasePath` and `tableName` values appropriately.
:::

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
    partitionSpec: partitionpath:VALUE
```
:::danger Note:
Add appropriate `sourceFormat`, `tableBasePath` and `tableName` values appropriately.
:::

</TabItem>
</Tabs>

From your terminal under the cloned onetable directory, run the sync process using the below command.

 ```shell md title="shell"
 java -jar utilities/target/utilities-0.1.0-SNAPSHOT-bundled.jar -datasetConfig my_config.yaml
 ```

:::tip Note:
At this point, if you check your bucket path, you will be able to see the `hoodie`, `_delta_log`, `metadata` directory
with metadata files which contains the information that helps query engines interpret the data as the target table.
:::

### Register the target table in Glue Data Catalog
From your terminal, create a glue database.
   
 ```shell md title="shell"
 aws glue create-database --database-input "{\"Name\":\"onetable_synced_db\"}"
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
aws glue create-crawler --name onetable_crawler --role arn:aws:iam::${accountId}:role/service-role/${roleName} --database onetable_synced_db --targets "{\"HudiTargets\":[{\"Paths\":[\"${s3DataPath}\"]}]}"
```

</TabItem>
<TabItem value="delta">

```shell md title="shell"
aws glue create-crawler --name onetable_crawler --role arn:aws:iam::${accountId}:role/service-role/${roleName} --database onetable_synced_db --targets "{\"DeltaTargets\":[{\"Paths\":[\"${s3DataPath}\"]}]}"
```

</TabItem>
<TabItem value="iceberg">

```shell md title="shell"
aws glue create-crawler --name onetable_crawler --role arn:aws:iam::${accountId}:role/service-role/${roleName} --database onetable_synced_db --targets "{\"IcebergTargets\":[{\"Paths\":[\"${s3DataPath}\"]}]}"
```

</TabItem>
</Tabs>

From your terminal, run the glue crawler.

```shell md title="shell"
 aws glue start-crawler --name onetable_crawler
```
Once the crawler succeeds, you’ll be able to query this Iceberg table from Athena,
EMR and/or Redshift query engines.

### Validating the results
Here’s how our table looks like in Amazon Athena.

![Iceberg Table in Amazon Athena](./static/athena-iceberg.png)

## Conclusion

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

In this guide we saw how to, 
1. sync a source table to create Hudi metadata with Onetable
2. catalog the data as a Hudi table in Glue Data Catalog
3. validate the Onetable synced Hudi table in Amazon Athena

</TabItem>
<TabItem value="delta">

In this guide we saw how to,
1. sync a source table to create Delta metadata with Onetable
2. catalog the data as a Delta table in Glue Data Catalog
3. validate the Onetable synced Delta table in Amazon Athena

</TabItem>
<TabItem value="iceberg">

In this guide we saw how to,
1. sync a source table to create Iceberg metadata with Onetable
2. catalog the data as an Iceberg table in Glue Data Catalog
3. validate the Onetable synced Iceberg table in Amazon Athena

</TabItem>
</Tabs>