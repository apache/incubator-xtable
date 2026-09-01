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
 
package org.apache.xtable.spark;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TestXTableSyncListener {

  @Test
  void matchesIdenticalAndTrailingSlash() {
    assertTrue(XTableSyncListener.pathsMatch("file:///wh/orders", "file:///wh/orders"));
    assertTrue(XTableSyncListener.pathsMatch("file:///wh/orders/", "file:///wh/orders"));
  }

  @Test
  void matchesPartitionSubPathEitherDirection() {
    assertTrue(XTableSyncListener.pathsMatch("file:///wh/orders/dt=2024", "file:///wh/orders"));
    assertTrue(XTableSyncListener.pathsMatch("file:///wh/orders", "file:///wh/orders/dt=2024"));
  }

  @Test
  void matchesAcrossSchemeWhenOneAuthorityMissing() {
    assertTrue(XTableSyncListener.pathsMatch("file:/wh/orders", "/wh/orders"));
  }

  @Test
  void doesNotMatchDifferentPaths() {
    assertFalse(XTableSyncListener.pathsMatch("file:///wh/orders", "file:///wh/customers"));
    // sibling prefix that is not a path boundary must not match
    assertFalse(XTableSyncListener.pathsMatch("file:///wh/orders_v2", "file:///wh/orders"));
  }

  @Test
  void doesNotMatchDifferentAuthorities() {
    assertFalse(XTableSyncListener.pathsMatch("s3://bucketA/wh/orders", "s3://bucketB/wh/orders"));
  }

  @Test
  void matchesSameAuthority() {
    assertTrue(
        XTableSyncListener.pathsMatch("s3://bucket/wh/orders/dt=1", "s3://bucket/wh/orders"));
  }
}
