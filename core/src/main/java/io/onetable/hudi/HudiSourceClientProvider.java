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
 
package io.onetable.hudi;

import java.util.Map;

import lombok.extern.log4j.Log4j2;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;

import io.onetable.client.SourceClientProvider;
import io.onetable.client.SourceTable;

/** A concrete implementation of {@link SourceClientProvider} for Hudi table format. */
@Log4j2
public class HudiSourceClientProvider extends SourceClientProvider<HoodieInstant> {

  @Override
  public HudiClient getSourceClientInstance(
      SourceTable sourceTableConfig, Map<String, String> clientConfigs) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder()
            .setConf(hadoopConf)
            .setBasePath(sourceTableConfig.getBasePath())
            .setLoadActiveTimelineOnLoad(true)
            .build();
    if (!metaClient.getTableConfig().getTableType().equals(HoodieTableType.COPY_ON_WRITE)) {
      log.warn("Source table is Merge On Read. Only base files will be synced");
    }

    final HudiSourcePartitionSpecExtractor sourcePartitionSpecExtractor =
        HudiSourceConfigImpl.fromProperties(clientConfigs).loadSourcePartitionSpecExtractor();

    return new HudiClient(metaClient, sourcePartitionSpecExtractor);
  }
}
