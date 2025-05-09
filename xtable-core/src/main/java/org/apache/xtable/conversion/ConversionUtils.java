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
 
package org.apache.xtable.conversion;

import static org.apache.xtable.model.storage.TableFormat.DELTA;
import static org.apache.xtable.model.storage.TableFormat.HUDI;
import static org.apache.xtable.model.storage.TableFormat.ICEBERG;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.table.timeline.HoodieInstant;

import org.apache.xtable.delta.DeltaConversionSourceProvider;
import org.apache.xtable.hudi.HudiConversionSourceProvider;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;

public class ConversionUtils {

  public static SourceTable convertToSourceTable(TargetTable table) {
    return new SourceTable(
        table.getName(),
        table.getFormatName(),
        table.getBasePath(),
        table.getBasePath(),
        table.getNamespace(),
        table.getCatalogConfig(),
        table.getAdditionalProperties());
  }

  public static ConversionSourceProvider<?> getConversionSourceProvider(
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
