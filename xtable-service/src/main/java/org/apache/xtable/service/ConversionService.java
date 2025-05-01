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
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.table.timeline.HoodieInstant;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.hadoop.HadoopTables;

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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class ConversionService {
  @Inject SparkHolder sparkHolder;

  public ConvertTableResponse convertTable(ConvertTableRequest request) {
    ConversionController conversionController =
        new ConversionController(sparkHolder.jsc().hadoopConfiguration());
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

    Pair<String, String> responseFields =
        getIcebergSchemaAndMetadataPath(
            request.getSourceTablePath(), sparkHolder.jsc().hadoopConfiguration());

    RestTargetTable internalTable =
        new RestTargetTable("ICEBERG", responseFields.getLeft(), responseFields.getRight());
    return new ConvertTableResponse(Collections.singletonList(internalTable));
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

  public static Pair<String, String> getIcebergSchemaAndMetadataPath(
      String tableLocation, Configuration conf) {
    HadoopTables tables = new HadoopTables(conf);
    Table table = tables.load(tableLocation);
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata current = ops.current();
    return Pair.of(current.metadataFileLocation(), SchemaParser.toJson(current.schema()));
  }
}
