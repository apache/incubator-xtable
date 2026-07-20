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
import java.util.Locale;
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
import org.apache.xtable.delta.DeltaConversionTargetConfig;
import org.apache.xtable.hudi.HudiConversionSourceProvider;
import org.apache.xtable.hudi.HudiSourceConfig;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;
import org.apache.xtable.kernel.DeltaKernelConversionSourceProvider;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncMode;
import org.apache.xtable.model.sync.SyncResult;
import org.apache.xtable.paimon.PaimonConversionSourceProvider;
import org.apache.xtable.parquet.ParquetConversionSourceProvider;

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
    Properties sourceProperties = new Properties();
    if (spec.getPartitionSpec() != null && !spec.getPartitionSpec().isEmpty()) {
      sourceProperties.put(HudiSourceConfig.PARTITION_FIELD_SPEC_CONFIG, spec.getPartitionSpec());
    }
    // The data files may live at a different path than the source table root (e.g. Iceberg keeps
    // them under <basePath>/data). Targets write their metadata alongside the data files, so the
    // target base path is the data path (required by Hudi), defaulting to the source base path.
    String dataPath =
        spec.getDataPath() != null && !spec.getDataPath().isEmpty()
            ? spec.getDataPath()
            : spec.getBasePath();
    SourceTable sourceTable =
        SourceTable.builder()
            .name(spec.getKey())
            .basePath(spec.getBasePath())
            .dataPath(dataPath)
            .namespace(spec.getNamespace())
            .formatName(spec.getSourceFormat())
            .additionalProperties(sourceProperties)
            .build();

    // In Kernel mode, set the property ConversionTargetFactory reads to pick the Delta Kernel
    // writer (ignored for non-Delta targets).
    List<TargetTable> targetTables =
        spec.getTargets().stream()
            .map(
                targetFormat -> {
                  Properties targetProperties = new Properties();
                  if (spec.isUseDeltaKernel()) {
                    targetProperties.setProperty(
                        DeltaConversionTargetConfig.USE_KERNEL, Boolean.TRUE.toString());
                  }
                  return TargetTable.builder()
                      .name(spec.getKey())
                      .basePath(dataPath)
                      .namespace(spec.getNamespace())
                      .formatName(targetFormat)
                      .additionalProperties(targetProperties)
                      .build();
                })
            .collect(Collectors.toList());

    ConversionConfig conversionConfig =
        ConversionConfig.builder()
            .sourceTable(sourceTable)
            .targetTables(targetTables)
            .syncMode(SyncMode.INCREMENTAL)
            .build();

    ConversionSourceProvider<?> sourceProvider =
        sourceProviderFor(spec.getSourceFormat(), spec.isUseDeltaKernel());
    sourceProvider.init(hadoopConf);

    log.info(
        "Running XTable sync for table {} ({} -> {}) at {}",
        spec.getKey(),
        spec.getSourceFormat(),
        spec.getTargets(),
        spec.getBasePath());
    return new ConversionController(hadoopConf).sync(conversionConfig, sourceProvider);
  }

  private static ConversionSourceProvider<?> sourceProviderFor(
      String sourceFormat, boolean useDeltaKernel) {
    switch (sourceFormat.toUpperCase(Locale.ROOT)) {
      case TableFormat.HUDI:
        return new HudiConversionSourceProvider();
      case TableFormat.DELTA:
        // The default delta-core reader is Spark-3.4-only; the Kernel reader is Spark-free.
        return useDeltaKernel
            ? new DeltaKernelConversionSourceProvider()
            : new DeltaConversionSourceProvider();
      case TableFormat.ICEBERG:
        return new IcebergConversionSourceProvider();
      case TableFormat.PAIMON:
        return new PaimonConversionSourceProvider();
      case TableFormat.PARQUET:
        return new ParquetConversionSourceProvider();
      default:
        throw new UnsupportedOperationException(
            "Unsupported source format for spark-runtime sync: " + sourceFormat);
    }
  }
}
