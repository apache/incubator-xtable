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
 
package io.onetable.filter;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import org.apache.arrow.util.VisibleForTesting;

import io.onetable.model.storage.OneDataFile;
import io.onetable.spi.filter.FilterInterceptor;

/**
 * This class is responsible for applying custom transformations at specific points in the format
 * translation flow using a configured list of {@link FilterInterceptor}s. On invocation, it applies
 * the filters on the given input in the order they were registered. The output of one filter is the
 * input of the next filter. Given that a filter can alter the input, a subsequent filter in the
 * chain would apply its transformation on the original input.
 *
 * <p>An instance of this class expects filters of a specific type (parameterized), which would all
 * operate on the same type of input. When a filter is registered, it is initialized with the
 * properties provided to this class. The registration can be done programmatically, or using the
 * ServiceLoader.
 *
 * <p>For e.g. an instance of the {@link FilterManager} responsible for transforming data file
 * before adding to the target client. The registered filters include a filter to ensure all paths
 * are relative, and a filter to emit the count of files added to the target client. At the entry
 * point of data file sync, this manager would invoke both the filters in that order. As such the
 * specific instance would expect the INPUT_TYPE to be list of {@link OneDataFile}.
 *
 * @param <FILTER_TYPE> the type of filter to manage
 * @param <INPUT_TYPE> the type of input to filter
 */
// this class is lombok.Data so that this class can be extended
@Data
@RequiredArgsConstructor
public class FilterManager<FILTER_TYPE extends FilterInterceptor<INPUT_TYPE>, INPUT_TYPE> {
  /** A list of all the filters registered with this manager */
  private final List<FILTER_TYPE> filters = new ArrayList<>();

  /**
   * A map of configurations of all the filters. This is passed to each filter when it is registered
   */
  private final Map<String, String> properties;

  public INPUT_TYPE process(INPUT_TYPE input) {
    for (FILTER_TYPE filter : filters) {
      input = filter.apply(input);
    }
    return input;
  }

  /**
   * Load filters of the given type using the ServiceLoader. If filterIdentifiers is not empty, only
   * the filters with the given identifiers are registered and in the order specified in the list.
   * If filterIdentifiers is empty, all the filters of the given type are registered.
   *
   * <p>Note: all previously registered filters are removed and closed before the new filters are
   * loaded.
   *
   * @param filterTypeClass the type of filters to load
   * @param filterIdentifiers the identifiers of the filters to register
   */
  public void loadFilters(Class<FILTER_TYPE> filterTypeClass, Set<String> filterIdentifiers) {
    filters.forEach(FilterInterceptor::close);
    filters.clear();

    Map<String, FILTER_TYPE> allFilters = getAvailableFilters(filterTypeClass);
    if (filterIdentifiers == null || filterIdentifiers.isEmpty()) {
      allFilters.values().forEach(this::registerFilter);
    } else {
      filterIdentifiers.stream()
          .filter(allFilters::containsKey)
          .map(allFilters::get)
          .forEach(this::registerFilter);
    }
  }

  /**
   * Register a filter with this manager. The filter is initialized with the properties provided to
   * this manager. The filter is added to the end of the list of filters. It will be invoked after
   * all the existing filters and will receive the output of the last filter as input.
   *
   * @param filter the filter to register
   */
  void registerFilter(FILTER_TYPE filter) {
    filter.init(properties);
    filters.add(filter);
  }

  @VisibleForTesting
  protected Map<String, FILTER_TYPE> getAvailableFilters(Class<FILTER_TYPE> filterTypeClass) {
    ServiceLoader<FILTER_TYPE> loader = ServiceLoader.load(filterTypeClass);
    Map<String, FILTER_TYPE> allFilters =
        StreamSupport.stream(loader.spliterator(), false)
            .collect(Collectors.toMap(FilterInterceptor::getIdentifier, f -> f));
    return allFilters;
  }

  @VisibleForTesting
  List<FILTER_TYPE> getFilters() {
    return Collections.unmodifiableList(filters);
  }
}
