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

import static org.apache.xtable.model.storage.TableFormat.DELTA;
import static org.apache.xtable.model.storage.TableFormat.HUDI;
import static org.apache.xtable.model.storage.TableFormat.ICEBERG;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.service.models.ConvertTableRequest;
import org.apache.xtable.service.models.ConvertTableResponse;
import org.apache.xtable.service.models.ConvertedTable;
import org.apache.xtable.service.spark.SparkHolder;
import org.apache.xtable.service.utils.ConversionServiceUtil;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Service for managing table format conversions.
 *
 * <p>It supports formats such as ICEBERG, HUDI, and DELTA. The conversion process involves creating
 * a source table, generating target tables, and then executing the conversion via a designated
 * conversion controller.
 */
@ApplicationScoped
public class ConversionService {
  private final SparkHolder sparkHolder;
  private final ConversionController conversionController;
  private final ConversionServiceUtil conversionServiceUtil;

  /**
   * Constructs a ConversionService instance with required dependencies.
   *
   * @param sparkHolder the Spark holder instance containing the Spark context and configuration
   * @param conversionServiceUtil utility for handling metadata operations
   */
  @Inject
  public ConversionService(SparkHolder sparkHolder, ConversionServiceUtil conversionServiceUtil) {
    this.sparkHolder = sparkHolder;
    this.conversionServiceUtil = conversionServiceUtil;
    this.conversionController = new ConversionController(sparkHolder.jsc().hadoopConfiguration());
  }

  /**
   * Constructs a ConversionService instance using dependency injection for testing.
   *
   * @param sparkHolder the Spark holder instance
   * @param conversionController a preconfigured conversion controller
   * @param conversionServiceUtil utility for handling metadata operations
   */
  @VisibleForTesting
  public ConversionService(
      SparkHolder sparkHolder,
      ConversionServiceUtil conversionServiceUtil,
      ConversionController conversionController) {
    this.sparkHolder = sparkHolder;
    this.conversionController = conversionController;
    this.conversionServiceUtil = conversionServiceUtil;
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

    ConversionSourceProvider<?> conversionSourceProvider =
        conversionServiceUtil.getConversionSourceProvider(
            convertTableRequest.getSourceFormat(), sparkHolder.jsc().hadoopConfiguration());

    conversionController.sync(conversionConfig, conversionSourceProvider);

    List<ConvertedTable> restTargetTables = new ArrayList<>();
    for (String targetFormat : convertTableRequest.getTargetFormats()) {
      if (targetFormat.equals(ICEBERG)) {
        Pair<String, String> responseFields =
            conversionServiceUtil.getIcebergSchemaAndMetadataPath(
                convertTableRequest.getSourceTablePath(), sparkHolder.jsc().hadoopConfiguration());
        ConvertedTable icebergTable =
            new ConvertedTable(ICEBERG, responseFields.getLeft(), responseFields.getRight());
        restTargetTables.add(icebergTable);
      } else if (targetFormat.equals(HUDI)) {
        Pair<String, String> responseFields =
            conversionServiceUtil.getHudiSchemaAndMetadataPath(
                convertTableRequest.getSourceTablePath(), sparkHolder.jsc().hadoopConfiguration());
        ConvertedTable hudiTable =
            new ConvertedTable(HUDI, responseFields.getLeft(), responseFields.getRight());
        restTargetTables.add(hudiTable);
      } else if (targetFormat.equals(DELTA)) {
        Pair<String, String> responseFields =
            conversionServiceUtil.getDeltaSchemaAndMetadataPath(
                convertTableRequest.getSourceTablePath(), sparkHolder.spark());
        ConvertedTable deltaTable =
            new ConvertedTable(DELTA, responseFields.getLeft(), responseFields.getRight());
        restTargetTables.add(deltaTable);
      }
    }
    return new ConvertTableResponse(restTargetTables);
  }
}
