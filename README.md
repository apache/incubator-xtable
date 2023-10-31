# onetable
OneTable is an omni-directional converter for table formats that facilitates interoperability across data processing systems
and query engines.
Currently, OneTable supports widely adopted open-source table formats such as Apache Hudi, Apache Iceberg, and Delta Lake.

OneTable simplifies data lake operations by leveraging a common model for table representation.
This allows users to write data in one format while still benefiting from integrations and features available in other
formats.
For instance, OneTable enables existing Hudi users to seamlessly work with Databricks's Photon Engine or query Iceberg
tables with Snowflake.
Creating transformations from one format to another is straightforward and only requires the implementation of a few interfaces,
which we believe will facilitate the expansion of supported source and target formats in the future.

# Building the project and running tests.
1. Use Java11 for building the project. If you are using some other java version, you can use [jenv](https://github.com/jenv/jenv) to use multiple java versions locally.
2. Build the project using `mvn clean package`. Use `mvn clean package -DskipTests` to skip tests while building.
3. Use `mvn clean test` or `mvn test` to run all unit tests. If you need to run only a specific test you can do this
   by something like `mvn test -Dtest=TestDeltaSync -pl core`.
4. Similarly use `mvn clean verify` or `mvn verify` to run integration tests.

# Style guide
1. We use [Maven Spotless plugin](https://github.com/diffplug/spotless/tree/main/plugin-maven) and 
   [Google java format](https://github.com/google/google-java-format) for code style.
2. Use `mvn spotless:check` to find out code style violations and `mvn spotless:apply` to fix them. 
   Code style check is tied to compile phase by default, so code style violations will lead to build failures.

# Running the bundled jar
1. Get a pre-built bundled jar or create the jar with `mvn install -DskipTests`
2. create a yaml file that follows the format below:
```yaml
sourceFormat: HUDI
tableFormats:
  - DELTA
  - ICEBERG
datasets:
  -
    tableBasePath: s3://tpc-ds-datasets/1GB/hudi/call_center
    tablename: call_center
  -
    tableBasePath: s3://tpc-ds-datasets/1GB/hudi/catalog_sales
    tablename: catalog_sales
    partitionSpec: cs_sold_date_sk:VALUE
  -
    tableBasePath: s3://hudi/multi-partition-dataset
    tablename: multi_partition_dataset
    partitionSpec: time_millis:DAY:yyyy-MM-dd,type:VALUE
```
- `tableFormats` is a list of formats you want to create from your source Hudi tables
- `tableBasePath` is the basePath of the Hudi table
- `partitionSpec` is a spec that allows us to infer partition values. If the table is not partitioned, leave it blank. If it is partitioned, you can specify a spec with a comma separated list with format `path:type:format`
  - `path` is a dot separated path to the partition field
  - `type` describes how the partition value was generated from the column value
    - `VALUE`: an identity transform of field value to partition value
    - `YEAR`: data is partitioned by a field representing a date and year granularity is used
    - `MONTH`: same as `YEAR` but with month granularity
    - `DAY`: same as `YEAR` but with day granularity
    - `HOUR`: same as `YEAR` but with hour granularity
  - `format`: if your partition type is `YEAR`, `MONTH`, `DAY`, or `HOUR` specify the format for the date string as it appears in your file paths
3. The default implementations of table format clients can be replaced with custom implementations by specifying a client configs yaml file in the format below:
```yaml
# sourceClientProviderClass: The class name of a table format's client factory, where the client is
#     used for reading from a table of this format. All user configurations, including hadoop config
#     and client specific configuration, will be available to the factory for instantiation of the
#     client.
# targetClientProviderClass: The class name of a table format's client factory, where the client is
#     used for writing to a table of this format.
# configuration: A map of configuration values specific to this client.
tableFormatsClients:
    HUDI:
      sourceClientProviderClass: io.onetable.hudi.HudiSourceClientProvider
    DELTA:
      targetClientProviderClass: io.onetable.delta.DeltaClient
      configuration:
        spark.master: local[2]
        spark.app.name: onetableclient
```
4. run with `java -jar utilities/target/utilities-0.1.0-SNAPSHOT-bundled.jar --datasetConfig my_config.yaml [ --hadoopConfig hdfs-site.xml ] [--clientsConfig clients.yaml]`
The bundled jar includes hadoop dependencies for AWS and GCP. Authentication for AWS is done with 
`com.amazonaws.auth.DefaultAWSCredentialsProviderChain`. To override this setting, specify a different implementation 
with the `--awsCredentialsProvider` option.

# Contributing
## Setup
For setting up the repo on IntelliJ, open the project and change the java version to Java11 in File->ProjectStructure
![img.png](style/IDE.png)

You have found a bug, or have a cool idea you that want to contribute to the project ? Please file a GitHub issue [here](https://github.com/onetable-io/onetable/issues)

## Adding a new target format
Adding a new target format requires a developer implement [TargetClient](./api/src/main/java/io/onetable/spi/sync/TargetClient.java). Once you have implemented that interface, you can integrate it into the [OneTableClient](./core/src/main/java/io/onetable/client/OneTableClient.java). If you think others may find that target useful, please raise a Pull Request to add it to the project.

## Overview of the sync process
![img.png](docs/images/sync_flow.jpg)