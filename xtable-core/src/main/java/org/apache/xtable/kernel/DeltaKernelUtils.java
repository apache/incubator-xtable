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

import lombok.experimental.UtilityClass;

import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;

/**
 * Utility methods for working with Delta Kernel API.
 *
 * <p>This class provides common helper methods used across Delta Kernel integration components to
 * avoid code duplication and ensure consistent behavior.
 */
@UtilityClass
public class DeltaKernelUtils {

  /**
   * Checks if a Delta table exists at the specified path.
   *
   * <p>NOTE: This method loads the full snapshot, which reads and parses transaction log files.
   * This is heavyweight but reliable. A lighter approach using {@code
   * engine.getFileSystemClient().listFrom(basePath + "/_delta_log")} was attempted but had issues
   * with exception handling - {@code listFrom()} may throw different exception types depending on
   * the filesystem implementation.
   *
   * <p>This method only catches {@link TableNotFoundException}, allowing other exceptions (network
   * errors, permission issues, corrupted metadata) to propagate. This ensures real errors are
   * visible rather than being silently masked.
   *
   * @param engine the Delta Kernel engine to use
   * @param basePath the path to the Delta table
   * @return true if the table exists, false if it doesn't exist
   * @throws RuntimeException if there's an error other than table not found (e.g., network issues,
   *     permissions)
   */
  public static boolean tableExists(Engine engine, String basePath) {
    try {
      Table table = Table.forPath(engine, basePath);
      table.getLatestSnapshot(engine);
      return true;
    } catch (TableNotFoundException e) {
      // Expected: table doesn't exist yet
      return false;
    }
    // Let other exceptions propagate (network issues, permissions, corrupted metadata, etc.)
  }
}
