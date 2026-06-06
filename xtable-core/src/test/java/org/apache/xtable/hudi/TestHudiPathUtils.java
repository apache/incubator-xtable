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

package org.apache.xtable.hudi;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

public class TestHudiPathUtils {

  @Test
  void filterMetadataPaths_removesKnownMetadataDirs() {
    List<String> input = Arrays.asList("region=US", "_delta_log", ".hoodie", "year=2024");
    List<String> result = HudiPathUtils.filterMetadataPaths(input);
    assertEquals(Arrays.asList("region=US", "year=2024"), result);
  }

  @Test
  void filterMetadataPaths_nestedMetadataDirAsLastSegment() {
    List<String> input = Arrays.asList("year=2024/_delta_log", "region=US/.hoodie");
    List<String> result = HudiPathUtils.filterMetadataPaths(input);
    assertEquals(Collections.emptyList(), result);
  }

  @Test
  void filterMetadataPaths_keepsEmptyString() {
    List<String> input = Arrays.asList("", "region=US");
    List<String> result = HudiPathUtils.filterMetadataPaths(input);
    assertEquals(Arrays.asList("", "region=US"), result);
  }

  @Test
  void filterMetadataPaths_keepsPartitionStartingWithUnderscore() {
    List<String> input = Arrays.asList("_status=active", "_year=2024", "region=US");
    List<String> result = HudiPathUtils.filterMetadataPaths(input);
    assertEquals(Arrays.asList("_status=active", "_year=2024", "region=US"), result);
  }

  @Test
  void filterMetadataPaths_keepsPartitionStartingWithDot() {
    List<String> input = Arrays.asList(".version=1", "region=US");
    List<String> result = HudiPathUtils.filterMetadataPaths(input);
    assertEquals(Arrays.asList(".version=1", "region=US"), result);
  }
}