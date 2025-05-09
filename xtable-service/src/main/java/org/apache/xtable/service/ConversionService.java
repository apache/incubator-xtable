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

import java.util.ArrayList;
import java.util.List;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.SchemaParser;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.avro.AvroSchemaConverter;
import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionUtils;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
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

  /**
   * Constructs a ConversionService instance with required dependencies.
   *
   * @param serviceConfig the conversion service configuration
   */
  @Inject
  public ConversionService(ConversionServiceConfig serviceConfig) {
    this.serviceConfig = serviceConfig;
    this.hadoopConf = getHadoopConf();
    this.conversionController = new ConversionController(hadoopConf);
  }

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
   * Constructs a ConversionService instance using dependency injection for testing.
   *
   * @param conversionController a preconfigured conversion controller
   */
  @VisibleForTesting
  public ConversionService(
      ConversionServiceConfig serviceConfig,
      ConversionController conversionController,
      Configuration hadoopConf) {
    this.serviceConfig = serviceConfig;
    this.conversionController = conversionController;
    this.hadoopConf = hadoopConf;
  }

  /**
   * Converts a source table to one or more target table formats.
   *
   * <p>The method builds a SourceTable based on the request parameters and constructs corresponding
   * TargetTable instances for each target format. It then performs a synchronous conversion through
   * the conversion controller. After conversion, it retrieves schema and metadata paths for each
   * target table.
   *
   * @param convertTableRequest the conversion request containing source table details and target
   *     formats
   * @return a response containing details of converted target tables
   */
  public ConvertTableResponse convertTable(ConvertTableRequest convertTableRequest) {
    SourceTable sourceTable =
        SourceTable.builder()
            .name(convertTableRequest.getSourceTableName())
            .basePath(convertTableRequest.getSourceTablePath())
            .formatName(convertTableRequest.getSourceFormat())
            .build();

    List<TargetTable> targetTables = new ArrayList<>();
    for (String targetFormat : convertTableRequest.getTargetFormats()) {
      TargetTable targetTable =
          TargetTable.builder()
              .name(convertTableRequest.getSourceTableName())
              .basePath(convertTableRequest.getSourceTablePath())
              .formatName(targetFormat)
              .build();
      targetTables.add(targetTable);
    }

    ConversionConfig conversionConfig =
        ConversionConfig.builder().sourceTable(sourceTable).targetTables(targetTables).build();

    conversionController.sync(
        conversionConfig,
        ConversionUtils.getConversionSourceProvider(
            convertTableRequest.getSourceFormat(), hadoopConf));

    List<ConvertedTable> convertedTables = new ArrayList<>();
    for (TargetTable targetTable : targetTables) {
      InternalTable internalTable =
          ConversionUtils.getConversionSourceProvider(targetTable.getFormatName(), hadoopConf)
              .getConversionSourceInstance(convertToSourceTable(targetTable))
              .getCurrentTable();
      String schemaString;
      switch (targetTable.getFormatName()) {
        case TableFormat.HUDI:
          schemaString =
              AvroSchemaConverter.getInstance()
                  .fromInternalSchema(internalTable.getReadSchema())
                  .toString();
          break;
        case TableFormat.ICEBERG:
          org.apache.iceberg.Schema iceSchema =
              IcebergSchemaExtractor.getInstance().toIceberg(internalTable.getReadSchema());
          schemaString = SchemaParser.toJson(iceSchema);
          break;
        case TableFormat.DELTA:
          schemaString =
              SparkSchemaExtractor.getInstance()
                  .fromInternalSchema(internalTable.getReadSchema())
                  .json();
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported table format: " + targetTable.getFormatName());
      }
      convertedTables.add(
          ConvertedTable.builder()
              .targetFormat(internalTable.getName())
              .targetSchema(schemaString)
              .targetMetadataPath(internalTable.getLatestMetdataPath())
              .build());
    }
    return new ConvertTableResponse(convertedTables);
  }
}
