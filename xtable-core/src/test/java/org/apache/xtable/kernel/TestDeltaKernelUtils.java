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

package org.apache.xtable.kernel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class TestDeltaKernelUtils {
  @Test
  void normalizeTablePathPreservesPlainLocalPath() {
    assertEquals("/local/data/path", DeltaKernelUtils.normalizeTablePath("/local/data//path"));
  }

  @Test
  void normalizeTablePathPreservesExplicitFileUri() {
    assertEquals(
        "file:///local/data/path", DeltaKernelUtils.normalizeTablePath("file:///local/data//path"));
  }

  @Test
  void normalizeTablePathKeepsWindowsLocalPathOutOfFileScheme() {
    String normalized = DeltaKernelUtils.normalizeTablePath("C:/local/data//path");
    assertFalse(normalized.startsWith("file:"));
  }

  @Test
  void normalizeTablePathHandlesNull() {
    assertNull(DeltaKernelUtils.normalizeTablePath(null));
  }
}
