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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

import io.onetable.spi.filter.FilterInterceptor;

class FilterManagerTest {

  private HashMap<String, FilterInterceptor<String>> availableFilters;
  private FilterInterceptor<String> mockFilter1;
  private FilterInterceptor<String> mockFilter2;
  private FilterInterceptor<String> mockFilter3;

  @BeforeEach
  void setup() {
    availableFilters = new HashMap<>();
    mockFilter1 = mock(FilterInterceptor.class);
    when(mockFilter1.getIdentifier()).thenReturn("mock1");
    availableFilters.put("mock1", mockFilter1);
    mockFilter2 = mock(FilterInterceptor.class);
    when(mockFilter2.getIdentifier()).thenReturn("mock2");
    availableFilters.put("mock2", mockFilter2);
    mockFilter3 = mock(FilterInterceptor.class);
    when(mockFilter3.getIdentifier()).thenReturn("mock3");
    availableFilters.put("mock3", mockFilter3);
  }

  @Test
  void process() {
    HashMap<String, String> properties = new HashMap<>();
    FilterManager<FilterInterceptor<String>, String> manager = new FilterManager<>(properties);
    FilterManager<FilterInterceptor<String>, String> spyManager = spy(manager);

    Class<FilterInterceptor<String>> filterType =
        (Class<FilterInterceptor<String>>) mockFilter1.getClass();
    doReturn(availableFilters).when(spyManager).getAvailableFilters(filterType);

    spyManager.loadFilters(filterType, Sets.newHashSet("mock1", "mock3"));

    when(mockFilter1.apply("test")).thenReturn("test1");
    when(mockFilter3.apply("test1")).thenReturn("test3");

    String result = spyManager.process("test");
    assertEquals("test3", result);
    verify(mockFilter1, times(1)).apply("test");
    verify(mockFilter2, never()).apply(any());
    verify(mockFilter3, times(1)).apply("test1");
  }

  @Test
  void loadFilters() {
    HashMap<String, String> properties = new HashMap<>();
    FilterManager<FilterInterceptor<String>, String> manager = new FilterManager<>(properties);
    FilterManager<FilterInterceptor<String>, String> spyManager = spy(manager);

    Class<FilterInterceptor<String>> filterType =
        (Class<FilterInterceptor<String>>) mockFilter1.getClass();
    doReturn(availableFilters).when(spyManager).getAvailableFilters(filterType);

    // test load all filters
    spyManager.loadFilters(filterType, null);
    List<FilterInterceptor<String>> filters = spyManager.getFilters();
    assertEquals(3, filters.size());
    assertTrue(filters.contains(mockFilter1));
    assertTrue(filters.contains(mockFilter2));
    assertTrue(filters.contains(mockFilter3));
    verify(mockFilter1, times(1)).init(properties);
    verify(mockFilter2, times(1)).init(properties);
    verify(mockFilter3, times(1)).init(properties);

    spyManager.loadFilters(filterType, Sets.newHashSet("mock1"));
    filters = spyManager.getFilters();
    assertEquals(1, filters.size());
    assertTrue(filters.contains(mockFilter1));
    verify(mockFilter1, times(2)).init(properties);
    verify(mockFilter1, times(1)).close();
    verify(mockFilter2, times(1)).close();

    spyManager.loadFilters(filterType, Sets.newHashSet("mock1", "mock3"));
    filters = spyManager.getFilters();
    assertEquals(2, filters.size());
    assertTrue(filters.contains(mockFilter1));
    assertTrue(filters.contains(mockFilter3));
  }

  @Test
  void registerFilter() {
    HashMap<String, String> properties = new HashMap<>();
    FilterManager<FilterInterceptor<String>, String> manager = new FilterManager<>(properties);
    assertTrue(manager.getFilters().isEmpty());

    manager.registerFilter(mockFilter1);
    List<FilterInterceptor<String>> filters = manager.getFilters();
    assertEquals(1, filters.size());
    assertEquals(mockFilter1, filters.get(0));
    verify(mockFilter1, times(1)).init(properties);

    manager.registerFilter(mockFilter2);
    filters = manager.getFilters();
    assertEquals(2, filters.size());
    assertEquals(mockFilter1, filters.get(0));
    assertEquals(mockFilter2, filters.get(1));
    verify(mockFilter1, times(1)).init(properties);
    verify(mockFilter2, times(1)).init(properties);
  }
}
