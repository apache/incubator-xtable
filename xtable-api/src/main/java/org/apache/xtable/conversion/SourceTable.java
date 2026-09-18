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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Properties;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

import org.apache.hadoop.conf.Configuration;

@EqualsAndHashCode(callSuper = true)
@Getter
public class SourceTable extends ExternalTable {
  /** The path to the data files, defaults to the basePath */
  @NonNull private final String dataPath;

  @Builder(toBuilder = true)
  public SourceTable(
      String name,
      String formatName,
      String basePath,
      String dataPath,
      String[] namespace,
      CatalogConfig catalogConfig,
      Properties additionalProperties,
      Configuration hadoopConf) {
    super(name, formatName, basePath, namespace, catalogConfig, additionalProperties, hadoopConf);
    this.dataPath = dataPath == null ? this.getBasePath() : sanitizeBasePath(dataPath);
  }

  public static SourceTable withDetectedFormat(
      String name,
      String basePath,
      String dataPath,
      String[] namespace,
      CatalogConfig catalogConfig,
      Properties additionalProperties,
      Configuration hadoopConf) {

    Configuration resolvedConf = hadoopConf != null ? hadoopConf : new Configuration();
    String detectedFormat = resolveFormatOrThrow(basePath, resolvedConf);

    return SourceTable.builder()
        .name(name)
        .formatName(detectedFormat)
        .basePath(basePath)
        .dataPath(dataPath)
        .namespace(namespace)
        .catalogConfig(catalogConfig)
        .additionalProperties(additionalProperties)
        .hadoopConf(resolvedConf)
        .build();
  }

  private static String resolveFormatOrThrow(String basePath, Configuration hadoopConf) {
    try {
      return SourceTableFormatDetector.detectFormat(basePath, hadoopConf);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to auto-detect source table format", e);
    }
  }
}
