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
 
package org.apache.xtable.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.catalog.HierarchicalTableIdentifier;
import org.apache.xtable.model.catalog.ThreePartHierarchicalTableIdentifier;

public class TestCatalogUtils {

  @Test
  void testCastToHierarchicalTableIdentifier() {
    // Valid HierarchicalTableIdentifier objects
    HierarchicalTableIdentifier catalogTableIdentifier1 =
        ThreePartHierarchicalTableIdentifier.fromDotSeparatedIdentifier("db.table");
    ThreePartHierarchicalTableIdentifier catalogTableIdentifier2 =
        ThreePartHierarchicalTableIdentifier.fromDotSeparatedIdentifier("catalog.db.table");

    // Invalid HierarchicalTableIdentifier object
    CatalogTableIdentifier catalogTableIdentifier3 = new TestCatalogTableIdentifier();

    HierarchicalTableIdentifier output;
    output = CatalogUtils.castToHierarchicalTableIdentifier(catalogTableIdentifier1);
    assertEquals("db.table", output.getId());

    output = CatalogUtils.castToHierarchicalTableIdentifier(catalogTableIdentifier2);
    assertEquals("catalog.db.table", output.getId());

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> CatalogUtils.castToHierarchicalTableIdentifier(catalogTableIdentifier3));
    assertEquals(
        "Invalid tableIdentifier implementation: org.apache.xtable.catalog.TestCatalogUtils$TestCatalogTableIdentifier",
        exception.getMessage());
  }

  static class TestCatalogTableIdentifier implements CatalogTableIdentifier {
    @Override
    public String getId() {
      return "test-catalog";
    }
  }
}
