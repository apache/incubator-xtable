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
 
package org.apache.xtable.catalog;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

public class CatalogUtils {

  private static final List<String> S3_FS_SCHEMES = Arrays.asList("s3", "s3a", "s3n");

  public static boolean hasStorageDescriptorLocationChanged(
      String storageDescriptorLocation, String tableBasePath) {

    if (StringUtils.isEmpty(storageDescriptorLocation)) {
      return true;
    }
    URI storageDescriptorUri = new Path(storageDescriptorLocation).toUri();
    URI basePathUri = new Path(tableBasePath).toUri();

    // In case of s3 path, compare without schemes
    boolean includeScheme =
        !S3_FS_SCHEMES.contains(basePathUri.getScheme())
            || !S3_FS_SCHEMES.contains(storageDescriptorUri.getScheme());
    storageDescriptorLocation = getPath(storageDescriptorUri, includeScheme);
    tableBasePath = getPath(basePathUri, includeScheme);
    return !Objects.equals(storageDescriptorLocation, tableBasePath);
  }

  private static String getPath(URI uri, boolean includeScheme) {
    if (includeScheme) {
      return uri.toString();
    }
    return uri.getAuthority() + uri.getPath();
  }
}
