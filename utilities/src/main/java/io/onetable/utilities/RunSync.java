/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package io.onetable.utilities;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.Data;
import lombok.extern.log4j.Log4j2;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.VisibleForTesting;

import com.fasterxml.jackson.annotation.JsonMerge;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.onetable.client.OneTableClient;
import io.onetable.client.PerTableConfig;
import io.onetable.client.SourceClientProvider;
import io.onetable.hudi.ConfigurationBasedPartitionSpecExtractor;
import io.onetable.hudi.HudiSourceConfig;
import io.onetable.model.storage.TableFormat;
import io.onetable.model.sync.SyncMode;
import io.onetable.utilities.RunSync.TableFormatClients.ClientConfig;

/**
 * Provides a standalone runner for the sync process. See README.md for more details on how to run
 * this.
 */
@Log4j2
public class RunSync {

  private static final String DATASET_CONFIG_OPTION = "d";
  private static final String HADOOP_CONFIG_PATH = "p";
  private static final String CLIENTS_CONFIG_PATH = "c";
  private static final String HELP_OPTION = "h";

  private static final Options OPTIONS =
      new Options()
          .addOption(
              DATASET_CONFIG_OPTION,
              "datasetConfig",
              true,
              "The path to a yaml file containing dataset configuration")
          .addOption(
              HADOOP_CONFIG_PATH,
              "hadoopConfig",
              true,
              "Hadoop config xml file path containing configs necessary to access the "
                  + "file system. These configs will override the default configs.")
          .addOption(
              CLIENTS_CONFIG_PATH,
              "clientsConfig",
              true,
              "The path to a yaml file containing onetable client configurations. "
                  + "These configs will override the default")
          .addOption(HELP_OPTION, "help", false, "Displays help information to run this utility");

  public static void main(String[] args) throws IOException, ParseException {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(OPTIONS, args);
    if (cmd.hasOption(HELP_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("RunSync", OPTIONS);
      return;
    }

    DatasetConfig datasetConfig = new DatasetConfig();
    try (InputStream inputStream =
        Files.newInputStream(Paths.get(cmd.getOptionValue(DATASET_CONFIG_OPTION)))) {
      ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
      ObjectReader objectReader = mapper.readerForUpdating(datasetConfig);
      objectReader.readValue(inputStream);
    }

    byte[] customConfig = getCustomConfigurations(cmd, HADOOP_CONFIG_PATH);
    Configuration hadoopConf = loadHadoopConf(customConfig);

    String sourceFormat = datasetConfig.sourceFormat;
    customConfig = getCustomConfigurations(cmd, CLIENTS_CONFIG_PATH);
    TableFormatClients tableFormatClients = loadTableFormatClientConfigs(customConfig);
    ClientConfig sourceClientConfig = tableFormatClients.getTableFormatsClients().get(sourceFormat);
    if (sourceClientConfig == null) {
      throw new IllegalArgumentException(
          String.format(
              "Source format %s is not supported. Known source and target formats are %s",
              sourceFormat, tableFormatClients.getTableFormatsClients().keySet()));
    }
    String sourceProviderClass = sourceClientConfig.sourceClientProviderClass;
    SourceClientProvider<?> sourceClientProvider = ReflectionUtils.loadClass(sourceProviderClass);
    sourceClientProvider.init(hadoopConf, sourceClientConfig.configuration);

    List<io.onetable.model.storage.TableFormat> tableFormatList =
        datasetConfig.getTargetFormats().stream()
            .map(TableFormat::valueOf)
            .collect(Collectors.toList());
    OneTableClient client = new OneTableClient(hadoopConf);
    for (DatasetConfig.Table table : datasetConfig.getDatasets()) {
      log.info(
          "Running sync for basePath {} for following table formats {}",
          table.getTableBasePath(),
          tableFormatList);
      PerTableConfig config =
          PerTableConfig.builder()
              .tableBasePath(table.getTableBasePath())
              .tableName(table.getTableName())
              .hudiSourceConfig(
                  HudiSourceConfig.builder()
                      .partitionSpecExtractorClass(
                          ConfigurationBasedPartitionSpecExtractor.class.getName())
                      .partitionFieldSpecConfig(table.getPartitionSpec())
                      .build())
              .targetTableFormats(tableFormatList)
              .syncMode(SyncMode.INCREMENTAL)
              .build();
      try {
        client.sync(config, sourceClientProvider);
      } catch (Exception e) {
        log.error(String.format("Error running sync for %s", table.getTableBasePath()), e);
      }
    }
  }

  private static byte[] getCustomConfigurations(CommandLine cmd, String option) throws IOException {
    byte[] customConfig = null;
    if (cmd.hasOption(option)) {
      customConfig = Files.readAllBytes(Paths.get(cmd.getOptionValue(option)));
    }
    return customConfig;
  }

  @VisibleForTesting
  static Configuration loadHadoopConf(byte[] customConfig) {
    Configuration conf = new Configuration();
    conf.addResource("onetable-hadoop-defaults.xml");
    if (customConfig != null) {
      conf.addResource(new ByteArrayInputStream(customConfig), "customConfigStream");
    }
    return conf;
  }

  /**
   * Loads the client configs. The method first loads the default configs and then merges any custom
   * configs provided by the user.
   *
   * @param customConfigs the custom configs provided by the user
   * @return available tableFormatsClients and their configs
   */
  @VisibleForTesting
  static TableFormatClients loadTableFormatClientConfigs(byte[] customConfigs) throws IOException {
    // get resource stream from default client config yaml file
    try (InputStream inputStream =
        RunSync.class.getClassLoader().getResourceAsStream("onetable-client-defaults.yaml")) {
      ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
      TableFormatClients clients = mapper.readValue(inputStream, TableFormatClients.class);
      if (customConfigs != null) {
        mapper.readerForUpdating(clients).readValue(customConfigs);
      }
      return clients;
    }
  }

  @Data
  public static class DatasetConfig {

    /**
     * Table format of the source table. This is a {@link TableFormat} value. Although the format of
     * the source can be auto-detected, it is recommended to specify it explicitly for cases where
     * the directory contains metadata of multiple formats.
     */
    String sourceFormat;

    /** The target formats to sync to. This is a list of {@link TableFormat} values. */
    List<String> targetFormats;

    /** Configuration of the dataset to sync, path, table name, etc. */
    List<Table> datasets;

    @Data
    public static class Table {
      /**
       * The base path of the table to sync. Any authentication configuration needed by HDFS client
       * can be provided using hadoop config file
       */
      String tableBasePath;

      String tableName;
      String partitionSpec;
    }
  }

  @Data
  public static class TableFormatClients {
    /** Map of table format name to the client configs. */
    @JsonProperty("tableFormatsClients")
    @JsonMerge
    Map<String, ClientConfig> tableFormatsClients;

    @Data
    public static class ClientConfig {
      /** The class name of the source client which reads the table metadata. */
      String sourceClientProviderClass;

      /** The class name of the target client which writes the table metadata. */
      String targetClientProviderClass;

      /** the configuration specific to the table format. */
      @JsonMerge Map<String, String> configuration;
    }
  }
}
