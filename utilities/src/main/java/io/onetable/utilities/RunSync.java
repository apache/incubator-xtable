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
  private static final String AWS_CREDENTIALS_OPTION = "a";
  private static final Options OPTIONS =
      new Options()
          .addOption(
              CONFIG_OPTION,
              "configFilePath",
              true,
              "The path to a yaml file containing your configuration")
          .addOption(
              AWS_CREDENTIALS_OPTION,
              "awsCredentialsProvider",
              true,
              "AWS Credentials Provider to use. Defaults to: com.amazonaws.auth.DefaultAWSCredentialsProviderChain");

  public static void main(String[] args) throws IOException, ParseException {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(OPTIONS, args);
    try (InputStream inputStream =
        Files.newInputStream(Paths.get(cmd.getOptionValue(CONFIG_OPTION)))) {
      ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
      DatasetConfig datasetConfig = new DatasetConfig();
      ObjectReader objectReader = mapper.readerForUpdating(datasetConfig);
      objectReader.readValue(inputStream);

      String awsCredentialsProvider =
          cmd.hasOption(AWS_CREDENTIALS_OPTION)
              ? cmd.getOptionValue(AWS_CREDENTIALS_OPTION)
              : "com.amazonaws.auth.DefaultAWSCredentialsProviderChain";
      Configuration conf = new Configuration();
      // Local Spark.
      conf.set("spark.master", "local[2]");
      conf.set("parquet.avro.write-old-list-structure", "false");
      conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
      // AWS permission dependencies.
      conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      conf.set("fs.s3.aws.credentials.provider", awsCredentialsProvider);
      conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      conf.set("fs.s3a.aws.credentials.provider", awsCredentialsProvider);
      // GCP dependencies
      conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
      conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");

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
  }

  @Data
  public static class DatasetConfig {
    List<String> tableFormats;
    List<Table> dataset;

    @Data
    public static class Table {
      String tableBasePath;
      String partitionSpec;
    }
  }
}
