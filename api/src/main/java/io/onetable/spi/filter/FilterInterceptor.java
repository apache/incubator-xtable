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
 
package io.onetable.spi.filter;

import java.util.Map;

/**
 * A {@link FilterInterceptor} is the filter component of the Intercepting Filter design pattern. It
 * is used to apply a transformation or to instrument a given input. Concrete implementations of
 * this interface are injected (intercept) in the table format translation workflow to customize the
 * behavior of the default translation workflow, or to generate logs or metrics.
 *
 * <p>E.g. of filters 1) a filter to convert absolute paths of data files to relative paths 2) a
 * filter to emit metrics for the number of data files in a snapshot 3) validate the table metadata
 * and throw an exception if certain conditions are not met
 *
 * <p>A filter to generate relative paths may be needed by certain query planners. In such a case
 * the user would inject such a filter before data files are written in the target format. Different
 * users may need to integrate with different metric collectors. A user can inject a specific filter
 * and limit the number of dependencies in the core code.
 *
 * <p>As such, the filter is a tool to customize the behavior of the table format translation
 * workflow.
 */
public interface FilterInterceptor<T> {
  /**
   * Each filter is identifiable by a name. This name can be used in config files to specify which
   * filters to apply at runtime.
   *
   * @return the identifier of this filter.
   */
  String getIdentifier();

  /**
   * Initialize the filter with the given properties.
   *
   * @param properties the properties map to initialize the filter.
   */
  void init(Map<String, String> properties);

  /**
   * Apply the filter to the given input. Note that this method may alter the input.
   *
   * @param input the input to apply the filter to.
   * @return the transformed input.
   */
  T apply(T input);

  /** Close the filter and release any resources. */
  default void close() {}
}
