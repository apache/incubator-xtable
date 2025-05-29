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
 
package org.apache.xtable.service;

import static org.apache.xtable.conversion.ConversionUtils.convertToSourceTable;
import static org.apache.xtable.hudi.HudiSourceConfig.PARTITION_FIELD_SPEC_CONFIG;
import static org.apache.xtable.model.storage.TableFormat.DELTA;
import static org.apache.xtable.model.storage.TableFormat.HUDI;
import static org.apache.xtable.model.storage.TableFormat.ICEBERG;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.table.timeline.HoodieInstant;

import org.apache.iceberg.SchemaParser;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.avro.AvroSchemaConverter;
import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.delta.DeltaConversionSourceProvider;
import org.apache.xtable.hudi.HudiConversionSourceProvider;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;
import org.apache.xtable.iceberg.IcebergSchemaExtractor;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.schema.SparkSchemaExtractor;
import org.apache.xtable.service.models.ConvertTableRequest;
import org.apache.xtable.service.models.ConvertTableResponse;
import org.apache.xtable.service.models.ConvertedTable;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Service for managing table format conversions.
 *
 * <p>It supports formats such as ICEBERG, HUDI, and DELTA. The conversion process involves creating
 * a source table, generating target tables, and then executing the conversion via a designated
 * conversion controller.
 */
@Log4j2
@ApplicationScoped
public class ConversionService {
  private final ConversionController conversionController;
  private final ConversionServiceConfig serviceConfig;
  private final Configuration hadoopConf;
  private final Map<String, ConversionSourceProvider<?>> sourceProviders;

  /**
   * Constructs a ConversionService instance with required dependencies.
   *
   * <p>This constructor initializes the ConversionService using the provided service configuration.
   * It retrieves the Hadoop configuration, creates a new ConversionController with the Hadoop
   * configuration, and initializes conversion source providers based on the Hadoop configuration.
   *
   * @param serviceConfig the conversion service configuration
   */
  @Inject
  public ConversionService(ConversionServiceConfig serviceConfig) {
    this.serviceConfig = serviceConfig;
    this.hadoopConf = getHadoopConf();
    this.conversionController = new ConversionController(hadoopConf);
    this.sourceProviders = initSourceProviders(hadoopConf);
  }

  /**
   * Retrieves the Hadoop configuration.
   *
   * <p>This method creates a new {@code Configuration} instance, reads the Hadoop configuration
   * file path from the service configuration, and attempts to load the configuration from the
   * specified XML file. If no resources are loaded, it logs a warning. If an error occurs during
   * configuration loading, it logs an error message.
   *
   * @return the initialized Hadoop {@code Configuration}
   */
  private Configuration getHadoopConf() {
    Configuration conf = new Configuration();
    String hadoopConfigPath = serviceConfig.getHadoopConfigPath();
    try {
      // Load configuration from the specified XML file
      conf.addResource(hadoopConfigPath);

      // If the resource wasn’t found, log a warning
      if (conf.size() == 0) {
        log.warn(
            "Could not load Hadoop configuration from: {}. Using default Hadoop configuration.",
            hadoopConfigPath);
      }
    } catch (Exception e) {
      log.error(
          "Error loading Hadoop configuration from: {}. Exception: {}",
          hadoopConfigPath,
          e.getMessage(),
          e);
    }
    return conf;
  }

  /**
   * Initializes conversion source providers for different table formats using the provided Hadoop
   * configuration.
   *
   * <p>This method creates and initializes source providers for HUDI, DELTA, and ICEBERG formats.
   * Each provider is initialized with the given Hadoop configuration and then mapped to its
   * respective table format identifier.
   *
   * @param hadoopConf the Hadoop configuration used to initialize the source providers
   * @return a map mapping table format identifiers to their corresponding initialized conversion
   *     source providers
   */
  private Map<String, ConversionSourceProvider<?>> initSourceProviders(Configuration hadoopConf) {
    Map<String, ConversionSourceProvider<?>> sourceProviders = new HashMap<>();
    ConversionSourceProvider<HoodieInstant> hudiConversionSourceProvider =
        new HudiConversionSourceProvider();
    ConversionSourceProvider<Long> deltaConversionSourceProvider =
        new DeltaConversionSourceProvider();
    ConversionSourceProvider<org.apache.iceberg.Snapshot> icebergConversionSourceProvider =
        new IcebergConversionSourceProvider();

    hudiConversionSourceProvider.init(hadoopConf);
    deltaConversionSourceProvider.init(hadoopConf);
    icebergConversionSourceProvider.init(hadoopConf);

    sourceProviders.put(HUDI, hudiConversionSourceProvider);
    sourceProviders.put(DELTA, deltaConversionSourceProvider);
    sourceProviders.put(ICEBERG, icebergConversionSourceProvider);

    return sourceProviders;
  }

  /**
   * Constructs a new ConversionService instance for testing purposes.
   *
   * <p>This constructor is visible for testing using dependency injection. It allows the injection
   * of a preconfigured ConversionController, Hadoop configuration, and source providers.
   *
   * @param serviceConfig the conversion service configuration
   * @param conversionController a preconfigured conversion controller
   * @param hadoopConf the Hadoop configuration to be used for initializing resources
   * @param sourceProviders a map of conversion source providers keyed by table format
   */
  @VisibleForTesting
  public ConversionService(
      ConversionServiceConfig serviceConfig,
      ConversionController conversionController,
      Configuration hadoopConf,
      Map<String, ConversionSourceProvider<?>> sourceProviders) {
    this.serviceConfig = serviceConfig;
    this.conversionController = conversionController;
    this.hadoopConf = hadoopConf;
    this.sourceProviders = sourceProviders;
  }

  /**
   * Converts a source table to one or more target table formats.
   *
   * @param convertTableRequest the conversion request containing source table details and target
   *     formats
   * @return a ConvertTableResponse containing details of the converted target tables
   */
  public ConvertTableResponse convertTable(ConvertTableRequest convertTableRequest) {

    Properties sourceProperties = new Properties();
    if (convertTableRequest.getConfigurations() != null) {
      String partitionSpec =
          convertTableRequest.getConfigurations().getOrDefault("partition-spec", null);
      if (partitionSpec != null) {
        sourceProperties.put(PARTITION_FIELD_SPEC_CONFIG, partitionSpec);
      }
    }

    SourceTable sourceTable =
        SourceTable.builder()
            .name(convertTableRequest.getSourceTableName())
            .basePath(convertTableRequest.getSourceTablePath())
            .dataPath(convertTableRequest.getSourceDataPath())
            .formatName(convertTableRequest.getSourceFormat())
            .additionalProperties(sourceProperties)
            .build();

    List<TargetTable> targetTables = new ArrayList<>();
    for (String targetFormat : convertTableRequest.getTargetFormats()) {
      TargetTable targetTable =
          TargetTable.builder()
              .name(convertTableRequest.getSourceTableName())
              // set the metadata path to the data path as the default (required by Hudi)
              .basePath(convertTableRequest.getSourceDataPath())
              .formatName(targetFormat)
              .additionalProperties(sourceProperties)
              .build();
      targetTables.add(targetTable);
    }

    ConversionConfig conversionConfig =
        ConversionConfig.builder().sourceTable(sourceTable).targetTables(targetTables).build();

    conversionController.sync(
        conversionConfig, sourceProviders.get(convertTableRequest.getSourceFormat()));

    List<ConvertedTable> convertedTables = new ArrayList<>();
    for (TargetTable targetTable : targetTables) {
      InternalTable internalTable =
          sourceProviders
              .get(targetTable.getFormatName())
              .getConversionSourceInstance(convertToSourceTable(targetTable))
              .getCurrentTable();
      String schemaString = extractSchemaString(targetTable, internalTable);
      convertedTables.add(
          ConvertedTable.builder()
              .targetFormat(internalTable.getTableFormat())
              .targetSchema(schemaString)
              .targetMetadataPath(internalTable.getLatestMetdataPath())
              .build());
    }
    return new ConvertTableResponse(convertedTables);
  }

  /**
   * Extracts the schema string from the given internal table based on the target table format.
   *
   * <p>This method supports the following table formats:
   *
   * <ul>
   *   <li><b>HUDI</b>: Converts the internal schema to an Avro schema and returns its string
   *       representation.
   *   <li><b>ICEBERG</b>: Converts the internal schema to an Iceberg schema and returns its JSON
   *       representation.
   *   <li><b>DELTA</b>: Converts the internal schema to a Spark schema and returns its JSON
   *       representation.
   * </ul>
   *
   * @param targetTable the target table containing the desired format information
   * @param internalTable the internal table from which the schema is read
   * @return the string representation of the converted schema
   * @throws UnsupportedOperationException if the target table format is not supported
   */
  private String extractSchemaString(TargetTable targetTable, InternalTable internalTable) {
    switch (targetTable.getFormatName()) {
      case TableFormat.HUDI:
        return AvroSchemaConverter.getInstance()
            .fromInternalSchema(internalTable.getReadSchema())
            .toString();
      case TableFormat.ICEBERG:
        org.apache.iceberg.Schema iceSchema =
            IcebergSchemaExtractor.getInstance().toIceberg(internalTable.getReadSchema());
        return SchemaParser.toJson(iceSchema);
      case TableFormat.DELTA:
        return SparkSchemaExtractor.getInstance()
            .fromInternalSchema(internalTable.getReadSchema())
            .json();
      default:
        throw new UnsupportedOperationException(
            "Unsupported table format: " + targetTable.getFormatName());
    }
  }
}
