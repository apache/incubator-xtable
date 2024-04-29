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
 
package org.apache.xtable.hudi;

import lombok.extern.log4j.Log4j2;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;

import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.PerTableConfig;

/** A concrete implementation of {@link ConversionSourceProvider} for Hudi table format. */
@Log4j2
public class HudiConversionSourceProvider extends ConversionSourceProvider<HoodieInstant> {

  @Override
  public HudiConversionSource getConversionSourceInstance(PerTableConfig sourceTableConfig) {
    this.sourceTableConfig = sourceTableConfig;
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder()
            .setConf(hadoopConf)
            .setBasePath(this.sourceTableConfig.getTableBasePath())
            .setLoadActiveTimelineOnLoad(true)
            .build();
    if (!metaClient.getTableConfig().getTableType().equals(HoodieTableType.COPY_ON_WRITE)) {
      log.warn("Source table is Merge On Read. Only base files will be synced");
    }

    final HudiSourcePartitionSpecExtractor sourcePartitionSpecExtractor =
        (HudiSourcePartitionSpecExtractor)
            sourceTableConfig.getHudiSourceConfig().loadSourcePartitionSpecExtractor();

    return new HudiConversionSource(metaClient, sourcePartitionSpecExtractor);
  }
}
