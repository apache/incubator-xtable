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
 
package org.apache.xtable.glue;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import org.apache.xtable.exception.ConfigurationException;
import org.apache.xtable.reflection.ReflectionUtils;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;

/**
 * Factory class for creating and configuring instances of {@link GlueClient} with settings provided
 * by {@link GlueCatalogConfig}.
 *
 * <p>This factory is responsible for setting the AWS region and credentials for the Glue client. If
 * a custom credentials provider class is specified in {@code GlueCatalogConfig}, it will use
 * reflection to instantiate the provider; otherwise, it defaults to the standard AWS credentials
 * provider.
 */
public class DefaultGlueClientFactory extends GlueClientFactory {

  public DefaultGlueClientFactory(GlueCatalogConfig glueConfig) {
    super(glueConfig);
  }

  public GlueClient getGlueClient() {
    GlueClientBuilder builder = GlueClient.builder();
    if (!StringUtils.isEmpty(glueConfig.getRegion())) {
      builder.region(Region.of(glueConfig.getRegion()));
    }

    AwsCredentialsProvider credentialsProvider;
    if (!StringUtils.isEmpty(glueConfig.getClientCredentialsProviderClass())) {
      String className = glueConfig.getClientCredentialsProviderClass();
      try {
        credentialsProvider =
            ReflectionUtils.createInstanceOfClassFromStaticMethod(
                className,
                "create",
                new Class<?>[] {Map.class},
                new Object[] {glueConfig.getClientCredentialsProviderConfigs()});
      } catch (ConfigurationException e) {
        // retry credentialsProvider creation without arguments if not a ClassNotFoundException
        if (e.getCause() instanceof ClassNotFoundException) {
          throw e;
        }
        credentialsProvider =
            ReflectionUtils.createInstanceOfClassFromStaticMethod(className, "create");
      }
    } else {
      credentialsProvider = DefaultCredentialsProvider.create();
    }

    builder.credentialsProvider(credentialsProvider);
    return builder.build();
  }
}
