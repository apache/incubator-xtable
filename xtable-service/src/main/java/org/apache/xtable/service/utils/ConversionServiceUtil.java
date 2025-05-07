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
 
package org.apache.xtable.service.utils;

import static org.apache.xtable.model.storage.TableFormat.DELTA;
import static org.apache.xtable.model.storage.TableFormat.HUDI;
import static org.apache.xtable.model.storage.TableFormat.ICEBERG;

import java.io.IOException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.hadoop.HadoopTables;

import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;

import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.delta.DeltaConversionSourceProvider;
import org.apache.xtable.hudi.HudiConversionSourceProvider;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ConversionServiceUtil {

  public Pair<String, String> getDeltaSchemaAndMetadataPath(
      String basePath, SparkSession sparkSession) {
    DeltaLog deltaLog = DeltaLog.forTable(sparkSession, basePath);
    Snapshot snapshot = deltaLog.snapshot();
    StructType schema = snapshot.metadata().schema();
    String metadataPath = snapshot.path().toString();
    String schemaStr = schema.json();
    return Pair.of(metadataPath, schemaStr);
  }

  public Pair<String, String> getHudiSchemaAndMetadataPath(
      String basePath, Configuration hadoopConf) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(basePath).setConf(hadoopConf).build();
    HoodieTimeline commits =
        metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    if (commits.empty()) {
      throw new IllegalStateException("No completed commits found in " + basePath);
    }
    HoodieInstant lastInstant = commits.lastInstant().get();
    String metaDir = metaClient.getMetaPath();
    String fileName = lastInstant.getFileName();
    String hudiLatestCommitPath = String.join("/", basePath, metaDir, fileName);

    Option<byte[]> raw = metaClient.getActiveTimeline().getInstantDetails(lastInstant);
    HoodieCommitMetadata commitMetadata;
    try {
      commitMetadata = HoodieCommitMetadata.fromBytes(raw.get(), HoodieCommitMetadata.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    String hudiSchemaStr = commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
    if (hudiSchemaStr == null) {
      throw new IllegalStateException("Commit " + lastInstant + " does not contain a schema");
    }
    return Pair.of(hudiLatestCommitPath, hudiSchemaStr);
  }

  public Pair<String, String> getIcebergSchemaAndMetadataPath(
      String tableLocation, Configuration conf) {
    HadoopTables tables = new HadoopTables(conf);
    Table table = tables.load(tableLocation);
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata current = ops.current();
    return Pair.of(current.metadataFileLocation(), SchemaParser.toJson(current.schema()));
  }

  public ConversionSourceProvider<?> getConversionSourceProvider(
      String sourceTableFormat, Configuration hadoopConf) {
    if (sourceTableFormat.equalsIgnoreCase(HUDI)) {
      ConversionSourceProvider<HoodieInstant> hudiConversionSourceProvider =
          new HudiConversionSourceProvider();
      hudiConversionSourceProvider.init(hadoopConf);
      return hudiConversionSourceProvider;
    } else if (sourceTableFormat.equalsIgnoreCase(DELTA)) {
      ConversionSourceProvider<Long> deltaConversionSourceProvider =
          new DeltaConversionSourceProvider();
      deltaConversionSourceProvider.init(hadoopConf);
      return deltaConversionSourceProvider;
    } else if (sourceTableFormat.equalsIgnoreCase(ICEBERG)) {
      ConversionSourceProvider<org.apache.iceberg.Snapshot> icebergConversionSourceProvider =
          new IcebergConversionSourceProvider();
      icebergConversionSourceProvider.init(hadoopConf);
      return icebergConversionSourceProvider;
    } else {
      throw new IllegalArgumentException("Unsupported source format: " + sourceTableFormat);
    }
  }
}
