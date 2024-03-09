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
 
package io.onetable.client;

import java.util.ServiceLoader;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.hadoop.conf.Configuration;

import io.onetable.exception.NotSupportedException;
import io.onetable.spi.sync.TargetClient;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TableFormatClientFactory {
  private static final TableFormatClientFactory INSTANCE = new TableFormatClientFactory();

  public static TableFormatClientFactory getInstance() {
    return INSTANCE;
  }

  /**
   * Create a fully initialized instance of the TargetClient represented by the given Table Format
   * name. Initialization is done with the config provideed through PerTableConfig and Conifuration
   * params.
   *
   * @param tableFormat
   * @param perTableConfig
   * @param configuration
   * @return
   */
  public TargetClient createForFormat(
      String tableFormat, PerTableConfig perTableConfig, Configuration configuration) {
    TargetClient targetClient = createTargetClientForName(tableFormat);

    targetClient.init(perTableConfig, configuration);
    return targetClient;
  }

  /**
   * Create an instance of the TargetClient via the default no-arg constructor. Expectation is that
   * target client specific settings may be provided prior to the calling of TargetClient.init()
   * which must be called prior to actual use of the target client returned by this factory method.
   *
   * @param tableFormatName
   * @return
   */
  public TargetClient createTargetClientForName(String tableFormatName) {
    ServiceLoader<TargetClient> loader = ServiceLoader.load(TargetClient.class);
    for (TargetClient target : loader) {
      if (target.getTableFormat().equalsIgnoreCase(tableFormatName)) {
        return target;
      }
    }
    throw new NotSupportedException("Target format is not yet supported: " + tableFormatName);
  }
}
