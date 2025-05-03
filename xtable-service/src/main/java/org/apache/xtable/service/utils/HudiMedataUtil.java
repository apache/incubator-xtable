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

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;

import java.io.IOException;

@ApplicationScoped
public class HudiMedataUtil {
    public Pair<String, String> getHudiSchemaAndMetadataPath(String basePath,  Configuration hadoopConf) {
        // Get latest commit
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                .setBasePath(basePath)
                .setConf(hadoopConf)
                .build();
        HoodieTimeline commits = metaClient.getActiveTimeline()
                .getCommitsTimeline()
                .filterCompletedInstants();
        if (commits.empty()) {
            throw new IllegalStateException("No completed commits found in " + basePath);
        }
        HoodieInstant lastInstant = commits.lastInstant().get();
        String metaDir = metaClient.getMetaPath();
        String fileName = lastInstant.getFileName();
        String hudiLatestCommitPath = String.join("/", basePath, metaDir, fileName);

        // Get latest schema
        Option<byte[]> raw = metaClient.getActiveTimeline()
                .getInstantDetails(lastInstant);
        HoodieCommitMetadata commitMetadata;
        try {
            commitMetadata = HoodieCommitMetadata
                    .fromBytes(raw.get(), HoodieCommitMetadata.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String hudiSchemaStr = commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
        if (hudiSchemaStr == null) {
            throw new IllegalStateException("Commit " + lastInstant + " does not contain a schema");
        }
        return Pair.of(hudiLatestCommitPath, hudiSchemaStr);
    }
}
