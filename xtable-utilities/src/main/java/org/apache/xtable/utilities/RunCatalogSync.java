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
 
package org.apache.xtable.utilities;

import static org.apache.xtable.utilities.RunSync.getCustomConfigurations;
import static org.apache.xtable.utilities.RunSync.loadHadoopConf;
import static org.apache.xtable.utilities.RunSync.loadTableFormatConversionConfigs;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.Data;
import lombok.extern.log4j.Log4j2;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.xtable.catalog.CatalogConversionFactory;
import org.apache.xtable.catalog.ExternalCatalogConfigFactory;
import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetCatalogConfig;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.sync.SyncMode;
import org.apache.xtable.reflection.ReflectionUtils;
import org.apache.xtable.spi.extractor.CatalogConversionSource;
import org.apache.xtable.utilities.RunCatalogSync.DatasetConfig.StorageIdentifier;
import org.apache.xtable.utilities.RunCatalogSync.DatasetConfig.TargetTableIdentifier;

/**
 * Provides standalone process for reading tables from a source catalog and synchronizing their
 * state in target tables, supports table format conversion as well if the target table chooses a
 * different format from source table.
 */
@Log4j2
public class RunCatalogSync {
  public static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final String CATALOG_SOURCE_AND_TARGET_CONFIG_PATH = "catalogConfig";
  private static final String HADOOP_CONFIG_PATH = "hadoopConfig";
  private static final String CONVERTERS_CONFIG_PATH = "convertersConfig";
  private static final String HELP_OPTION = "h";
  private static final Map<String, ConversionSourceProvider> CONVERSION_SOURCE_PROVIDERS =
      new HashMap<>();

  private static final Options OPTIONS =
      new Options()
          .addRequiredOption(
              CATALOG_SOURCE_AND_TARGET_CONFIG_PATH,
              "catalogSyncConfig",
              true,
              "The path to a yaml file containing source and target tables catalog configurations along with the table identifiers that need to synced")
          .addOption(
              HADOOP_CONFIG_PATH,
              "hadoopConfig",
              true,
              "Hadoop config xml file path containing configs necessary to access the "
                  + "file system. These configs will override the default configs.")
          .addOption(
              CONVERTERS_CONFIG_PATH,
              "convertersConfig",
              true,
              "The path to a yaml file containing InternalTable converter configurations. "
                  + "These configs will override the default")
          .addOption(HELP_OPTION, "help", false, "Displays help information to run this utility");

  public static void main(String[] args) throws Exception {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(OPTIONS, args);
    } catch (ParseException e) {
      new HelpFormatter().printHelp("xtable.jar", OPTIONS, true);
      return;
    }

    if (cmd.hasOption(HELP_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("RunCatalogSync", OPTIONS);
      return;
    }

    DatasetConfig datasetConfig = new DatasetConfig();
    try (InputStream inputStream =
        Files.newInputStream(
            Paths.get(cmd.getOptionValue(CATALOG_SOURCE_AND_TARGET_CONFIG_PATH)))) {
      ObjectReader objectReader = YAML_MAPPER.readerForUpdating(datasetConfig);
      objectReader.readValue(inputStream);
    }

    byte[] customConfig = getCustomConfigurations(cmd, HADOOP_CONFIG_PATH);
    Configuration hadoopConf = loadHadoopConf(customConfig);

    customConfig = getCustomConfigurations(cmd, CONVERTERS_CONFIG_PATH);
    RunSync.TableFormatConverters tableFormatConverters =
        loadTableFormatConversionConfigs(customConfig);

    Map<String, DatasetConfig.Catalog> catalogsByName =
        datasetConfig.getTargetCatalogs().stream()
            .collect(Collectors.toMap(DatasetConfig.Catalog::getCatalogName, Function.identity()));
    ExternalCatalogConfig sourceCatalogConfig = getCatalogConfig(datasetConfig.getSourceCatalog());
    CatalogConversionSource catalogConversionSource =
        CatalogConversionFactory.createCatalogConversionSource(sourceCatalogConfig, hadoopConf);
    ConversionController conversionController = new ConversionController(hadoopConf);
    for (DatasetConfig.Dataset dataset : datasetConfig.getDatasets()) {
      SourceTable sourceTable = null;
      if (dataset.getSourceCatalogTableIdentifier().getStorageIdentifier() != null) {
        StorageIdentifier storageIdentifier =
            dataset.getSourceCatalogTableIdentifier().getStorageIdentifier();
        sourceTable =
            SourceTable.builder()
                .name(storageIdentifier.getTableName())
                .basePath(storageIdentifier.getTableBasePath())
                .namespace(
                    storageIdentifier.getNamespace() == null
                        ? null
                        : storageIdentifier.getNamespace().split("\\."))
                .dataPath(storageIdentifier.getTableDataPath())
                .formatName(storageIdentifier.getTableFormat())
                .build();
      } else {
        sourceTable =
            catalogConversionSource.getSourceTable(
                dataset.getSourceCatalogTableIdentifier().getCatalogTableIdentifier());
      }
      List<TargetTable> targetTables = new ArrayList<>();
      Map<String, List<TargetCatalogConfig>> targetCatalogs = new HashMap<>();
      for (TargetTableIdentifier targetCatalogTableIdentifier :
          dataset.getTargetCatalogTableIdentifiers()) {
        TargetTable targetTable =
            TargetTable.builder()
                .name(sourceTable.getName())
                .basePath(sourceTable.getBasePath())
                .namespace(sourceTable.getNamespace())
                .formatName(targetCatalogTableIdentifier.getTableFormat())
                .build();
        targetTables.add(targetTable);
        if (!targetCatalogs.containsKey(targetTable.getId())) {
          targetCatalogs.put(targetTable.getId(), new ArrayList<>());
        }
        targetCatalogs
            .get(targetTable.getId())
            .add(
                TargetCatalogConfig.builder()
                    .catalogTableIdentifier(
                        targetCatalogTableIdentifier.getCatalogTableIdentifier())
                    .catalogConfig(
                        getCatalogConfig(
                            catalogsByName.get(targetCatalogTableIdentifier.getCatalogName())))
                    .build());
      }
      ConversionConfig conversionConfig =
          ConversionConfig.builder()
              .sourceTable(sourceTable)
              .targetTables(targetTables)
              .targetCatalogs(targetCatalogs)
              .syncMode(SyncMode.INCREMENTAL)
              .build();
      List<String> tableFormats =
          new ArrayList<>(Collections.singleton(sourceTable.getFormatName()));
      tableFormats.addAll(
          targetTables.stream().map(TargetTable::getFormatName).collect(Collectors.toList()));
      tableFormats = tableFormats.stream().distinct().collect(Collectors.toList());
      try {
        conversionController.syncTableAcrossCatalogs(
            conversionConfig,
            getConversionSourceProviders(tableFormats, tableFormatConverters, hadoopConf));
      } catch (Exception e) {
        log.error(String.format("Error running sync for %s", sourceTable.getBasePath()), e);
      }
    }
  }

  static ExternalCatalogConfig getCatalogConfig(DatasetConfig.Catalog catalog) {
    if (!StringUtils.isEmpty(catalog.getCatalogType())) {
      return ExternalCatalogConfigFactory.fromCatalogType(
          catalog.getCatalogType(), catalog.getCatalogName(), catalog.getCatalogProperties());
    } else {
      return ExternalCatalogConfig.builder()
          .catalogName(catalog.getCatalogName())
          .catalogImpl(catalog.getCatalogImpl())
          .catalogOptions(catalog.getCatalogProperties())
          .build();
    }
  }

  static Map<String, ConversionSourceProvider> getConversionSourceProviders(
      List<String> tableFormats,
      RunSync.TableFormatConverters tableFormatConverters,
      Configuration hadoopConf) {
    for (String tableFormat : tableFormats) {
      if (CONVERSION_SOURCE_PROVIDERS.containsKey(tableFormat)) {
        continue;
      }
      RunSync.TableFormatConverters.ConversionConfig sourceConversionConfig =
          tableFormatConverters.getTableFormatConverters().get(tableFormat);
      if (sourceConversionConfig == null) {
        throw new IllegalArgumentException(
            String.format(
                "Source format %s is not supported. Known source and target formats are %s",
                tableFormat, tableFormatConverters.getTableFormatConverters().keySet()));
      }
      String sourceProviderClass = sourceConversionConfig.conversionSourceProviderClass;
      ConversionSourceProvider<?> conversionSourceProvider =
          ReflectionUtils.createInstanceOfClass(sourceProviderClass);
      conversionSourceProvider.init(hadoopConf);
      CONVERSION_SOURCE_PROVIDERS.put(tableFormat, conversionSourceProvider);
    }
    return CONVERSION_SOURCE_PROVIDERS;
  }

  @Data
  public static class DatasetConfig {
    private Catalog sourceCatalog;
    private List<Catalog> targetCatalogs;
    private List<Dataset> datasets;

    @Data
    public static class Catalog {
      private String catalogName;
      private String catalogType;
      private String catalogImpl;
      private Map<String, String> catalogProperties;
    }

    @Data
    public static class StorageIdentifier {
      String tableFormat;
      String tableBasePath;
      String tableDataPath;
      String tableName;
      String partitionSpec;
      String namespace;
    }

    @Data
    public static class SourceTableIdentifier {
      CatalogTableIdentifier catalogTableIdentifier;
      StorageIdentifier storageIdentifier;
    }

    @Data
    public static class TargetTableIdentifier {
      String catalogName;
      String tableFormat;
      CatalogTableIdentifier catalogTableIdentifier;
    }

    @Data
    public static class Dataset {
      private SourceTableIdentifier sourceCatalogTableIdentifier;
      private List<TargetTableIdentifier> targetCatalogTableIdentifiers;
    }
  }
}
