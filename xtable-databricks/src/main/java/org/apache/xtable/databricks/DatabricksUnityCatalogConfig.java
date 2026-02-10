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
 
package org.apache.xtable.databricks;

import java.util.Map;

import lombok.Value;

import org.apache.xtable.conversion.ExternalCatalogConfig;

@Value
public class DatabricksUnityCatalogConfig {
  public static final String HOST = "externalCatalog.uc.host";
  public static final String WAREHOUSE_ID = "externalCatalog.uc.warehouseId";
  public static final String AUTH_TYPE = "externalCatalog.uc.authType";
  public static final String CLIENT_ID = "externalCatalog.uc.clientId";
  public static final String CLIENT_SECRET = "externalCatalog.uc.clientSecret";
  public static final String TOKEN = "externalCatalog.uc.token";

  String host;
  String warehouseId;
  String authType;
  String clientId;
  String clientSecret;
  String token;

  public static DatabricksUnityCatalogConfig from(ExternalCatalogConfig catalogConfig) {
    Map<String, String> props = catalogConfig.getCatalogProperties();
    return new DatabricksUnityCatalogConfig(
        props.get(HOST),
        props.get(WAREHOUSE_ID),
        props.get(AUTH_TYPE),
        props.get(CLIENT_ID),
        props.get(CLIENT_SECRET),
        props.get(TOKEN));
  }
}
