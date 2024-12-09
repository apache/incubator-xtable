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
 
package org.apache.xtable.spi.sync;

import java.net.URI;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

import org.apache.xtable.model.exception.CatalogRefreshException;

public class CatalogUtils {

  static boolean hasStorageDescriptorLocationChanged(
      String storageDescriptorLocation, String tableBasePath) {

    if (StringUtils.isEmpty(storageDescriptorLocation)) {
      return true;
    }
    URI storageDescriptorUri = new Path(storageDescriptorLocation).toUri();
    URI basePathUri = new Path(tableBasePath).toUri();

    if (storageDescriptorUri.equals(basePathUri)
        || storageDescriptorUri.getScheme().startsWith(basePathUri.getScheme())
        || basePathUri.getScheme().startsWith(storageDescriptorUri.getScheme())) {
      String storageDescriptorLocationIdentifier =
          storageDescriptorUri.getAuthority() + storageDescriptorUri.getPath();
      String tableBasePathIdentifier = basePathUri.getAuthority() + basePathUri.getPath();
      return !Objects.equals(storageDescriptorLocationIdentifier, tableBasePathIdentifier);
    }
    throw new CatalogRefreshException(
        String.format(
            "Storage scheme has changed for table catalogStorageDescriptorUri %s basePathUri %s",
            storageDescriptorUri, basePathUri));
  }
}
