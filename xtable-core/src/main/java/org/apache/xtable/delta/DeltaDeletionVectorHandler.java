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
 
package org.apache.xtable.delta;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import lombok.extern.log4j.Log4j2;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.storage.InternalDataFile;

@Log4j2
public final class DeltaDeletionVectorHandler {
  private final boolean allowUnsupportedDeletionVectors;
  private final Consumer<String> warningLogger;

  public DeltaDeletionVectorHandler(boolean allowUnsupportedDeletionVectors) {
    this(allowUnsupportedDeletionVectors, log::warn);
  }

  @VisibleForTesting
  DeltaDeletionVectorHandler(
      boolean allowUnsupportedDeletionVectors, Consumer<String> warningLogger) {
    this.allowUnsupportedDeletionVectors = allowUnsupportedDeletionVectors;
    this.warningLogger = warningLogger;
  }

  public void onDeletionVectorFound(String dataFilePath) {
    String message =
        String.format(
            "Delta deletion vectors are not supported by XTable conversion targets. "
                + "Data file %s contains a deletion vector.",
            dataFilePath);
    if (!allowUnsupportedDeletionVectors) {
      throw new NotSupportedException(
          message
              + " To ignore deletion vectors and continue with potentially inconsistent target "
              + "data, set "
              + DeltaConversionSourceConfig.ALLOW_UNSUPPORTED_DELETION_VECTORS
              + "=true.");
    }
    warningLogger.accept(
        message
            + " Continuing because "
            + DeltaConversionSourceConfig.ALLOW_UNSUPPORTED_DELETION_VECTORS
            + " is enabled. Target tables may contain rows that were deleted from the source.");
  }

  /**
   * Removes same-path add/remove pairs that represent deletion vector metadata updates rather than
   * physical data file changes.
   *
   * @param addedFiles data files added by the commit, keyed by path
   * @param removedFiles data files removed by the commit, keyed by path
   * @param dataFilesWithDeletionVectors paths of added data files containing deletion vectors
   */
  public static void removeDeletionVectorFileChanges(
      Map<String, InternalDataFile> addedFiles,
      Map<String, InternalDataFile> removedFiles,
      Set<String> dataFilesWithDeletionVectors) {
    for (String dataFilePath : dataFilesWithDeletionVectors) {
      if (removedFiles.containsKey(dataFilePath)) {
        addedFiles.remove(dataFilePath);
        removedFiles.remove(dataFilePath);
      } else {
        log.warn(
            "No Remove action found for the data file for which deletion vector is added {}. This is unexpected.",
            dataFilePath);
      }
    }
  }
}
