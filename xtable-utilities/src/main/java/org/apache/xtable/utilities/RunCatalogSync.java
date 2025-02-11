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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.log4j.Log4j2;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.xtable.catalog.CatalogConversionFactory;
import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetCatalogConfig;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.hudi.HudiSourceConfig;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.catalog.HierarchicalTableIdentifier;
import org.apache.xtable.model.catalog.ThreePartHierarchicalTableIdentifier;
import org.apache.xtable.model.storage.CatalogType;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncMode;
import org.apache.xtable.reflection.ReflectionUtils;
import org.apache.xtable.spi.extractor.CatalogConversionSource;
import org.apache.xtable.utilities.RunCatalogSync.DatasetConfig.StorageIdentifier;
import org.apache.xtable.utilities.RunCatalogSync.DatasetConfig.TableIdentifier;
import org.apache.xtable.utilities.RunCatalogSync.DatasetConfig.TargetTableIdentifier;
import org.apache.xtable.utilities.RunSync.TableFormatConverters;

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

    DatasetConfig datasetConfig;
    try (InputStream inputStream =
        Files.newInputStream(
            Paths.get(cmd.getOptionValue(CATALOG_SOURCE_AND_TARGET_CONFIG_PATH)))) {
      datasetConfig = YAML_MAPPER.readValue(inputStream, DatasetConfig.class);
    }

    byte[] customConfig = getCustomConfigurations(cmd, HADOOP_CONFIG_PATH);
    Configuration hadoopConf = loadHadoopConf(customConfig);

    customConfig = getCustomConfigurations(cmd, CONVERTERS_CONFIG_PATH);
    TableFormatConverters tableFormatConverters = loadTableFormatConversionConfigs(customConfig);

    Map<String, ExternalCatalogConfig> catalogsById =
        datasetConfig.getTargetCatalogs().stream()
            .collect(Collectors.toMap(ExternalCatalogConfig::getCatalogId, Function.identity()));
    Optional<CatalogConversionSource> catalogConversionSource =
        getCatalogConversionSource(datasetConfig.getSourceCatalog(), hadoopConf);
    ConversionController conversionController = new ConversionController(hadoopConf);
    for (DatasetConfig.Dataset dataset : datasetConfig.getDatasets()) {
      SourceTable sourceTable =
          getSourceTable(dataset.getSourceCatalogTableIdentifier(), catalogConversionSource);
      List<TargetTable> targetTables = new ArrayList<>();
      Map<TargetTable, List<TargetCatalogConfig>> targetCatalogs = new HashMap<>();
      for (TargetTableIdentifier targetCatalogTableIdentifier :
          dataset.getTargetCatalogTableIdentifiers()) {
        TargetTable targetTable =
            TargetTable.builder()
                .name(sourceTable.getName())
                .basePath(
                    getSourceTableLocation(
                        targetCatalogTableIdentifier.getTableFormat(), sourceTable))
                .namespace(sourceTable.getNamespace())
                .formatName(targetCatalogTableIdentifier.getTableFormat())
                .additionalProperties(sourceTable.getAdditionalProperties())
                .build();
        targetTables.add(targetTable);
        if (!targetCatalogs.containsKey(targetTable)) {
          targetCatalogs.put(targetTable, new ArrayList<>());
        }
        targetCatalogs
            .get(targetTable)
            .add(
                TargetCatalogConfig.builder()
                    .catalogTableIdentifier(
                        getCatalogTableIdentifier(
                            targetCatalogTableIdentifier.getTableIdentifier()))
                    .catalogConfig(catalogsById.get(targetCatalogTableIdentifier.getCatalogId()))
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
          Stream.concat(
                  Stream.of(sourceTable.getFormatName()),
                  targetTables.stream().map(TargetTable::getFormatName))
              .distinct()
              .collect(Collectors.toList());
      try {
        conversionController.syncTableAcrossCatalogs(
            conversionConfig,
            getConversionSourceProviders(tableFormats, tableFormatConverters, hadoopConf));
      } catch (Exception e) {
        log.error("Error running sync for {}", sourceTable.getBasePath(), e);
      }
    }
  }

  static Optional<CatalogConversionSource> getCatalogConversionSource(
      ExternalCatalogConfig sourceCatalog, Configuration hadoopConf) {
    if (CatalogType.STORAGE.equals(sourceCatalog.getCatalogType())) {
      return Optional.empty();
    }
    return Optional.of(
        CatalogConversionFactory.createCatalogConversionSource(sourceCatalog, hadoopConf));
  }

  static SourceTable getSourceTable(
      DatasetConfig.SourceTableIdentifier sourceTableIdentifier,
      Optional<CatalogConversionSource> catalogConversionSource) {
    SourceTable sourceTable = null;
    if (sourceTableIdentifier.getStorageIdentifier() != null) {
      StorageIdentifier storageIdentifier = sourceTableIdentifier.getStorageIdentifier();
      Properties sourceProperties = new Properties();
      if (storageIdentifier.getPartitionSpec() != null) {
        sourceProperties.put(
            HudiSourceConfig.PARTITION_FIELD_SPEC_CONFIG, storageIdentifier.getPartitionSpec());
      }
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
              .additionalProperties(sourceProperties)
              .build();
    } else if (catalogConversionSource.isPresent()) {
      TableIdentifier tableIdentifier = sourceTableIdentifier.getTableIdentifier();
      sourceTable =
          catalogConversionSource.get().getSourceTable(getCatalogTableIdentifier(tableIdentifier));
      if (tableIdentifier.getPartitionSpec() != null) {
        sourceTable
            .getAdditionalProperties()
            .put(HudiSourceConfig.PARTITION_FIELD_SPEC_CONFIG, tableIdentifier.getPartitionSpec());
      }
    }
    return sourceTable;
  }

  static String getSourceTableLocation(String targetTableFormat, SourceTable sourceTable) {
    return sourceTable.getFormatName().equals(TableFormat.ICEBERG)
            && targetTableFormat.equals(TableFormat.HUDI)
        ? sourceTable.getDataPath()
        : sourceTable.getBasePath();
  }

  static Map<String, ConversionSourceProvider> getConversionSourceProviders(
      List<String> tableFormats,
      TableFormatConverters tableFormatConverters,
      Configuration hadoopConf) {
    for (String tableFormat : tableFormats) {
      if (CONVERSION_SOURCE_PROVIDERS.containsKey(tableFormat)) {
        continue;
      }
      TableFormatConverters.ConversionConfig sourceConversionConfig =
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

  /**
   * Returns an implementation class for {@link CatalogTableIdentifier} based on the tableIdentifier
   * provided by user.
   */
  static CatalogTableIdentifier getCatalogTableIdentifier(TableIdentifier tableIdentifier) {
    if (tableIdentifier.getHierarchicalId() != null) {
      return ThreePartHierarchicalTableIdentifier.fromDotSeparatedIdentifier(
          tableIdentifier.getHierarchicalId());
    }
    throw new IllegalArgumentException("Invalid tableIdentifier configuration provided");
  }

  @Value
  @Builder
  @Jacksonized
  public static class DatasetConfig {
    /**
     * Configuration of the source catalog from which XTable will read. It must contain all the
     * necessary connection and access details for describing and listing tables
     */
    ExternalCatalogConfig sourceCatalog;
    /**
     * Defines configuration one or more target catalogs, to which XTable will write or update
     * tables. Unlike the source, these catalogs must be writable
     */
    List<ExternalCatalogConfig> targetCatalogs;
    /** A list of datasets that specify how a source table maps to one or more target tables. */
    List<Dataset> datasets;

    /** Configuration for catalog. */
    ExternalCatalogConfig catalogConfig;

    @Value
    @Builder
    @Jacksonized
    public static class Dataset {
      /** Identifies the source table in sourceCatalog. */
      SourceTableIdentifier sourceCatalogTableIdentifier;
      /** A list of one or more targets that this source table should be written to. */
      List<TargetTableIdentifier> targetCatalogTableIdentifiers;
    }

    @Value
    @Builder
    @Jacksonized
    public static class SourceTableIdentifier {
      /** Specifies the table identifier in the source catalog. */
      TableIdentifier tableIdentifier;
      /**
       * (Optional) Provides direct storage details such as a table’s base path (like an S3
       * location) and the partition specification. This allows reading from a source even if it is
       * not strictly registered in a catalog, as long as the format and location are known
       */
      StorageIdentifier storageIdentifier;
    }

    @Value
    @Builder
    @Jacksonized
    public static class TargetTableIdentifier {
      /**
       * The user defined unique identifier of the target catalog where the table will be created or
       * updated
       */
      String catalogId;
      /**
       * The target table format (e.g., DELTA, HUDI, ICEBERG), specifying how the data will be
       * stored at the target.
       */
      String tableFormat;
      /** Specifies the table identifier in the target catalog. */
      TableIdentifier tableIdentifier;
    }

    @Value
    @Builder
    @Jacksonized
    public static class TableIdentifier {
      /**
       * Specifics the three level hierarchical table identifier for {@link
       * HierarchicalTableIdentifier}
       */
      String hierarchicalId;
      /** Specifies the partition spec of the table */
      String partitionSpec;
    }

    /**
     * Configuration in storage for table. This is an optional field in {@link
     * SourceTableIdentifier}.
     */
    @Value
    @Builder
    @Jacksonized
    public static class StorageIdentifier {
      String tableFormat;
      String tableBasePath;
      String tableDataPath;
      String tableName;
      String partitionSpec;
      String namespace;
    }
  }
}
