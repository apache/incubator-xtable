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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;
import io.onetable.model.storage.OneDataFile;
import io.onetable.spi.filter.SnapshotFilesFilter;

/**
 * This filter will generate a new list of data files, in which the absolute paths of input data
 * files will be replaced by relative paths. This path conversion will be done only if the input
 * data files are located in the subtree with a configured base path as the root. For e.g. if the
 * configured base is located at /data/table1, and the input data files are located at
 * /data/table1/2019-01/abc.parquet, then the output data files will be located at
 * 2019-01/abc.parquet. However, if the input data files are located at
 * /data/table2/2019-01/abc.parquet, then the output data files will have the same path as the input
 * file, i.e. located at /data/table2/2019-01/abc.parquet.
 */
@AutoService(SnapshotFilesFilter.class)
public class ToRelativePathFilter implements SnapshotFilesFilter {
  private static final String BASE_PATH = ToRelativePathFilter.class.getSimpleName() + ".basePath";
  private String basePath;

  @Override
  public String getIdentifier() {
    return ToRelativePathFilter.class.getSimpleName();
  }

  @Override
  public void init(Map<String, String> properties) {
    basePath = properties.get(BASE_PATH);
  }

  /**
   * This method will perform the path conversion described in the contract of this filter.
   *
   * @param files a list of data files whose path needs to be converted
   * @return a new list of data files, with new file objects, whose path has been converted
   */
  @Override
  public List<OneDataFile> apply(List<OneDataFile> files) {
    List<OneDataFile> updatedFiles =
        files.stream()
            .map(
                file -> {
                  String oldPath = file.getPhysicalPath();
                  if (!oldPath.startsWith(basePath)) {
                    return file;
                  }

                  String newPath = oldPath.substring(basePath.length() + 1);

                  // create a new data file
                  return OneDataFile.builder()
                      .schemaVersion(file.getSchemaVersion())
                      .physicalPath(newPath)
                      .fileFormat(file.getFileFormat())
                      .partitionValues(file.getPartitionValues())
                      .fileSizeBytes(file.getFileSizeBytes())
                      .recordCount(file.getRecordCount())
                      .columnStats(file.getColumnStats())
                      .lastModified(file.getLastModified())
                      .build();
                })
            .collect(Collectors.toList());

    return updatedFiles;
  }
}
