---
sidebar_position: 1
---

# Creating your first interoperable table

:::danger Important
Using Onetable to sync your source tables in different target format involves running sync on your 
current dataset using a bundled jar. You can create this bundled jar by following the instructions 
on Onetable's [github](https://github.com/onetable-io/onetable#building-the-project-and-running-tests) page.
:::

In this tutorial we will look at how to use Onetable to add interoperability between table formats. 
For example, you can expose tables ingested with Hudi as an Iceberg and/or Delta Lake table without
copying or moving the underlying data files used for that table while maintaining a similar commit 
history to enable proper point in time queries.

## Pre-requisites:
1. A compute instance where you can run Apache Spark. This can be your local machine, docker, 
or a distributed service like Amazon EMR, Cloud Dataproc etc.
2. Clone the Onetable [repository](https://github.com/onetable-io/onetable) and create the 
`utilities-0.1.0-SNAPSHOT-bundled.jar` by following the steps [here](https://github.com/onetable-io/onetable#onetable).
3. Optional: Setup access to write to and/or read from distributed storage services like:
   * Amazon S3 by following the steps 
   [here](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) to install AWSCLIv2 
   and setup access credentials by following the steps
   [here](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html)
   * Google Cloud Storage by following the steps 
   [here](https://cloud.google.com/iam/docs/keys-create-delete#creating).

For the purpose of this tutorial, we will walk through the steps to using Onetable locally.

## Steps:

1. Initialize a `pyspark` shell.
   :::tip Note:
   You can choose to follow this example with `spark-sql` or `spark-shell` as well.
   :::
   
   ```shell md title="shell"
   pyspark \
   --packages org.apache.hudi:hudi-spark3.2-bundle_2.12:0.14.0 \
   --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
   --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
   ```

2. Write a Hudi table locally.
   ```python md title="pyspark"
   from pyspark.sql.types import *
   
   # initialize the bucket
   table_name = "trips_data"
   local_base_path = "/tmp/onetable-data"
   
   # generate data
   quickstart_utils = sc._jvm.org.apache.hudi.QuickstartUtils
   dataGen = quickstart_utils.DataGenerator()
   inserts = sc._jvm.org.apache.hudi.QuickstartUtils.convertToStringList(dataGen.generateInserts(10))
   
   # write to local
   df = spark.read \
   .json(spark.sparkContext.parallelize(inserts, 2))
   
   hudi_options = {
      'hoodie.table.name': table_name,
      'hoodie.datasource.write.partitionpath.field': 'partitionpath'
   }
   
   df.write.format("hudi") \
   .options(**hudi_options) \
   .mode("overwrite") \
   .save(f"{local_base_path}/{table_name}")
   ```

   :::tip Note: 
   If you instead want to write your table to Amazon S3 or Google Cloud Storage, follow the below steps:
   * For Amazon S3, follow the configurations specified [here](https://hudi.apache.org/docs/s3_hoodie/)
   * For Google Cloud Storage, follow the configurations specified [here](https://hudi.apache.org/docs/gcs_hoodie)
   :::

3. Create `my_config.yaml` in the cloned onetable directory.

   ```yaml  md title="yaml"
   sourceFormat: HUDI
   targetFormats:
     - DELTA
     - ICEBERG
   datasets:
     -
       tableBasePath: /tmp/onetable-data/hudi_trips_data
       tableName: hudi_trips_data
       partitionSpec: partitionpath:VALUE
   ```
   **Optional:** If your source table exists in Amazon S3 or Google Cloud Storage, you may use the below sample yaml file.

   ```yaml  md title="yaml"
   sourceFormat: HUDI
   targetFormats:
     - DELTA
     - ICEBERG
   datasets:
     -
       tableBasePath: s3://path/to/trips/data
       tableName: hudi_trips_data
       partitionSpec: partitionpath:VALUE
   ```

   :::tip Note:
   Authentication for AWS is done with `com.amazonaws.auth.DefaultAWSCredentialsProviderChain`. 
   To override this setting, specify a different implementation with the `--awsCredentialsProvider` option.
   
   Authentication for GCP requires service account credentials to be exported. i.e.
   `export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account_key.json`
   :::

4. In your terminal under the cloned onetable directory, run the below command.

   ```shell md title="shell"
   java -jar utilities/target/utilities-0.1.0-SNAPSHOT-bundled.jar -datasetConfig my_config.yaml
   ```

5. **Optional:**
   At this point, if you check your local path, you will be able to see `_delta_log` 
   and `metadata` directories with necessary log files that contain the schema, 
   commit history, partitions, and column stats that helps query engines to interpret the data 
   as a delta and/or iceberg table.

## Conclusion:
In this tutorial, we saw how to create a source table in Hudi format and
use Onetable to create metadata/log files that can be used to query the source table in Delta/Iceberg format.

## Next steps:
Go through the [Catalog Integration guides](https://link-to-guide) to add the Onetable synced tables
in different data catalogs.