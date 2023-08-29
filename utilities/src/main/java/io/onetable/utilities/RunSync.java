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
import java.util.stream.Collectors;

import lombok.Data;
import lombok.extern.log4j.Log4j2;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.util.VisibleForTesting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.onetable.client.OneTableClient;
import io.onetable.client.PerTableConfig;
import io.onetable.hudi.ConfigurationBasedPartitionSpecExtractor;
import io.onetable.hudi.HudiSourceConfig;
import io.onetable.model.storage.TableFormat;
import io.onetable.model.sync.SyncMode;

/**
 * Provides a standalone runner for the sync process. See README.md for more details on how to run
 * this.
 */
@Log4j2
public class RunSync {

  private static final String CONFIG_OPTION = "c";
  private static final String HADOOP_CONFIG_PATH = "h";
  private static final Options OPTIONS =
      new Options()
          .addOption(
              CONFIG_OPTION,
              "configFilePath",
              true,
              "The path to a yaml file containing your configuration")
          .addOption(
              HADOOP_CONFIG_PATH,
              "hadoopConfig",
              true,
              "Hadoop config xml file path containing configs necessary to access the "
                  + "file system. These configs will override the default configs.");

  public static void main(String[] args) throws IOException, ParseException {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(OPTIONS, args);

    DatasetConfig datasetConfig = new DatasetConfig();
    try (InputStream inputStream =
        Files.newInputStream(Paths.get(cmd.getOptionValue(CONFIG_OPTION)))) {
      ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
      ObjectReader objectReader = mapper.readerForUpdating(datasetConfig);
      objectReader.readValue(inputStream);
    }

    byte[] customConfig = null;
    if (cmd.hasOption(HADOOP_CONFIG_PATH)) {
      customConfig = Files.readAllBytes(Paths.get(cmd.getOptionValue(HADOOP_CONFIG_PATH)));
    }

    Configuration conf = loadHadoopConf(customConfig);

    List<TableFormat> tableFormatList =
        datasetConfig.getTableFormats().stream()
            .map(TableFormat::valueOf)
            .collect(Collectors.toList());
    OneTableClient client = new OneTableClient(conf);
    for (DatasetConfig.Table table : datasetConfig.getDataset()) {
      log.info(
          "Running sync for basePath {} for following table formats {}",
          table.getTableBasePath(),
          tableFormatList);
      PerTableConfig config =
          PerTableConfig.builder()
              .tableBasePath(table.getTableBasePath())
              .tableName(table.getTableBasePath())
              .hudiSourceConfig(
                  HudiSourceConfig.builder()
                      .partitionSpecExtractorClass(
                          ConfigurationBasedPartitionSpecExtractor.class.getName())
                      .partitionFieldSpecConfig(table.getPartitionSpec())
                      .build())
              .tableFormatsToSync(tableFormatList)
              .syncMode(SyncMode.INCREMENTAL)
              .build();
      try {
        client.sync(config);
      } catch (Exception e) {
        log.error(String.format("Error running sync for %s", table.getTableBasePath()), e);
      }
    }
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

  @Data
  public static class DatasetConfig {

    List<String> tableFormats;
    List<Table> dataset;

    @Data
    public static class Table {

      String tableBasePath;
      String tableName;
      String partitionSpec;
    }
  }
}
