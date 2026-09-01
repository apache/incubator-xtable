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
 
package org.apache.xtable.spark;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.delta.DeltaConversionSourceProvider;
import org.apache.xtable.hudi.HudiConversionSourceProvider;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncMode;
import org.apache.xtable.model.sync.SyncResult;

/**
 * Builds a {@link ConversionConfig} from a {@link TableSyncSpec} and runs an incremental {@link
 * ConversionController#sync} for it. This is the unit of work triggered by {@link
 * XTableSyncListener} after a successful write.
 *
 * <p>The sync watermark is persisted in the target's {@code TableSyncMetadata}, and {@link
 * SyncMode#INCREMENTAL} auto-falls back to a full snapshot when incremental is not safe (e.g. the
 * very first sync), so this call is idempotent and self-healing.
 */
@Log4j2
public class XTableSyncService {

  /** Runs a single sync for the given spec, returning the per-format results. */
  public Map<String, SyncResult> sync(TableSyncSpec spec, Configuration hadoopConf) {
    SourceTable sourceTable =
        SourceTable.builder()
            .name(spec.getKey())
            .basePath(spec.getBasePath())
            .dataPath(spec.getDataPath())
            .namespace(spec.getNamespace())
            .formatName(spec.getSourceFormat())
            .additionalProperties(new Properties())
            .build();

    List<TargetTable> targetTables =
        spec.getTargets().stream()
            .map(
                targetFormat ->
                    TargetTable.builder()
                        .name(spec.getKey())
                        .basePath(spec.getBasePath())
                        .namespace(spec.getNamespace())
                        .formatName(targetFormat)
                        .build())
            .collect(Collectors.toList());

    ConversionConfig conversionConfig =
        ConversionConfig.builder()
            .sourceTable(sourceTable)
            .targetTables(targetTables)
            .syncMode(SyncMode.INCREMENTAL)
            .build();

    ConversionSourceProvider<?> sourceProvider = sourceProviderFor(spec.getSourceFormat());
    sourceProvider.init(hadoopConf);

    log.info(
        "Running XTable sync for table {} ({} -> {}) at {}",
        spec.getKey(),
        spec.getSourceFormat(),
        spec.getTargets(),
        spec.getBasePath());
    return new ConversionController(hadoopConf).sync(conversionConfig, sourceProvider);
  }

  private static ConversionSourceProvider<?> sourceProviderFor(String sourceFormat) {
    switch (sourceFormat.toUpperCase()) {
      case TableFormat.HUDI:
        return new HudiConversionSourceProvider();
      case TableFormat.DELTA:
        return new DeltaConversionSourceProvider();
      case TableFormat.ICEBERG:
        return new IcebergConversionSourceProvider();
      default:
        throw new UnsupportedOperationException(
            "Unsupported source format for spark-runtime sync: " + sourceFormat);
    }
  }
}
