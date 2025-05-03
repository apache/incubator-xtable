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
import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.table.timeline.HoodieInstant;

import org.apache.iceberg.Snapshot;

import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.delta.DeltaConversionSourceProvider;
import org.apache.xtable.hudi.HudiConversionSourceProvider;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;
import org.apache.xtable.service.models.ConvertTableRequest;
import org.apache.xtable.service.models.ConvertTableResponse;
import org.apache.xtable.service.models.RestTargetTable;
import org.apache.xtable.service.spark.SparkHolder;
import org.apache.xtable.service.utils.DeltaMetadataUtil;
import org.apache.xtable.service.utils.HudiMedataUtil;
import org.apache.xtable.service.utils.IcebergMetadataUtil;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class ConversionService {
  private final SparkHolder sparkHolder;
  private final ConversionControllerFactory controllerFactory;
  private final IcebergMetadataUtil icebergUtil;
  private final HudiMedataUtil hudiUtil;
  private final DeltaMetadataUtil deltaUtil;

  @Inject
  public ConversionService(
      SparkHolder sparkHolder,
      ConversionControllerFactory controllerFactory,
      IcebergMetadataUtil icebergUtil,
      HudiMedataUtil hudiUtil,
      DeltaMetadataUtil deltaUtil) {
    this.sparkHolder = sparkHolder;
    this.controllerFactory = controllerFactory;
    this.icebergUtil = icebergUtil;
    this.hudiUtil = hudiUtil;
    this.deltaUtil = deltaUtil;
  }

  public ConvertTableResponse convertTable(ConvertTableRequest request) {
    Configuration conf = sparkHolder.jsc().hadoopConfiguration();
    ConversionController conversionController = controllerFactory.create(conf);

    SourceTable sourceTable =
        SourceTable.builder()
            .name(request.getSourceTableName())
            .basePath(request.getSourceTablePath())
            .formatName(request.getSourceFormat())
            .build();

    List<TargetTable> targetTables = new ArrayList<>();
    for (String targetFormat : request.getTargetFormats()) {
      TargetTable targetTable =
          TargetTable.builder()
              .name(request.getSourceTableName())
              .basePath(request.getSourceTablePath())
              .formatName(targetFormat)
              .build();
      targetTables.add(targetTable);
    }
    ConversionConfig conversionConfig =
        ConversionConfig.builder().sourceTable(sourceTable).targetTables(targetTables).build();
    ConversionSourceProvider<?> conversionSourceProvider =
        getConversionSourceProvider(request.getSourceFormat());
    conversionController.sync(conversionConfig, conversionSourceProvider);

    List<RestTargetTable> restTargetTables = new ArrayList<>();
    for (String targetFormat : request.getTargetFormats()) {
      if (targetFormat.equals("ICEBERG")) {
        Pair<String, String> responseFields =
                icebergUtil.getIcebergSchemaAndMetadataPath(
                        request.getSourceTablePath(), sparkHolder.jsc().hadoopConfiguration());
        RestTargetTable icebergTable =
                new RestTargetTable("ICEBERG", responseFields.getLeft(), responseFields.getRight());
        restTargetTables.add(icebergTable);
      } else if (targetFormat.equals("HUDI")) {
        Pair<String, String> responseFields =
                hudiUtil.getHudiSchemaAndMetadataPath(
                        request.getSourceTablePath(), sparkHolder.jsc().hadoopConfiguration());
        RestTargetTable hudiTable =
                new RestTargetTable("HUDI", responseFields.getLeft(), responseFields.getRight());
        restTargetTables.add(hudiTable);
      } else if(targetFormat.equals("DELTA")){
        Pair<String, String> responseFields =
                deltaUtil.getDeltaSchemaAndMetadataPath(
                        request.getSourceTablePath(), sparkHolder.spark());
        RestTargetTable deltaTable =
                new RestTargetTable("DELTA", responseFields.getLeft(), responseFields.getRight());
        restTargetTables.add(deltaTable);
      }
    }
    return new ConvertTableResponse(restTargetTables);
  }

  private ConversionSourceProvider<?> getConversionSourceProvider(String sourceTableFormat) {
    if (sourceTableFormat.equalsIgnoreCase(HUDI)) {
      ConversionSourceProvider<HoodieInstant> hudiConversionSourceProvider =
          new HudiConversionSourceProvider();
      hudiConversionSourceProvider.init(sparkHolder.jsc().hadoopConfiguration());
      return hudiConversionSourceProvider;
    } else if (sourceTableFormat.equalsIgnoreCase(DELTA)) {
      ConversionSourceProvider<Long> deltaConversionSourceProvider =
          new DeltaConversionSourceProvider();
      deltaConversionSourceProvider.init(sparkHolder.jsc().hadoopConfiguration());
      return deltaConversionSourceProvider;
    } else if (sourceTableFormat.equalsIgnoreCase(ICEBERG)) {
      ConversionSourceProvider<Snapshot> icebergConversionSourceProvider =
          new IcebergConversionSourceProvider();
      icebergConversionSourceProvider.init(sparkHolder.jsc().hadoopConfiguration());
      return icebergConversionSourceProvider;
    } else {
      throw new IllegalArgumentException("Unsupported source format: " + sourceTableFormat);
    }
  }
}
