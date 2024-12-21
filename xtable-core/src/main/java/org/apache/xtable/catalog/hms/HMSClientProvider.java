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
 
package org.apache.xtable.catalog.hms;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

import java.lang.reflect.InvocationTargetException;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import org.apache.hudi.common.util.VisibleForTesting;

@Log4j2
public class HMSClientProvider {

  private final HMSCatalogConfig hmsCatalogConfig;
  private final Configuration configuration;

  public HMSClientProvider(HMSCatalogConfig hmsCatalogConfig, Configuration configuration) {
    this.hmsCatalogConfig = hmsCatalogConfig;
    this.configuration = configuration;
  }

  @VisibleForTesting
  public IMetaStoreClient getMSC() throws MetaException, HiveException {
    HiveConf hiveConf = new HiveConf(configuration, HiveConf.class);
    hiveConf.set(METASTOREURIS.varname, hmsCatalogConfig.getServerUrl());
    IMetaStoreClient metaStoreClient;
    try {
      metaStoreClient =
          ((Hive)
                  Hive.class
                      .getMethod("getWithoutRegisterFns", HiveConf.class)
                      .invoke(null, hiveConf))
              .getMSC();
    } catch (NoSuchMethodException
        | IllegalAccessException
        | IllegalArgumentException
        | InvocationTargetException ex) {
      metaStoreClient = Hive.get(hiveConf).getMSC();
    }
    log.debug("Connected to metastore with uri: {}", hmsCatalogConfig.getServerUrl());
    return metaStoreClient;
  }
}
