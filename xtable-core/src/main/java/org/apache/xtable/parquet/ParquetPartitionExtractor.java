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
 
package org.apache.xtable.parquet;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.xtable.utilities.RunSync.DatasetConfig.Table.InputPartitionFields;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.schema.SchemaFieldFinder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ParquetPartitionExtractor {
  private static final ParquetPartitionExtractor INSTANCE = new ParquetPartitionExtractor();

  public static ParquetPartitionExtractor getInstance() {
    return INSTANCE;
  }
  public static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

// todo this is to be put inside RunSync.java
  public InputPartitionFields getPartitionsFromUserConfiguration(String configPath) throws IOException {
    InputPartitionFields partitionConfiguration = new InputPartitionFields();
    try (InputStream inputStream = Files.newInputStream(Paths.get(configPath))) {
      ObjectReader objectReader = YAML_MAPPER.readerForUpdating(partitionConfiguration);
      objectReader.readValue(inputStream);
      return partitionConfiguration;
    }
  }

  // TODO logic is too complicated can be simplified
/*  public List<PartitionValue> getPartitionValue(
      String basePath,
      String filePath,
      InternalSchema schema,
      Map<String, List<String>> partitionInfo) {
    List<PartitionValue> partitionValues = new ArrayList<>();
    java.nio.file.Path base = Paths.get(basePath).normalize();
    java.nio.file.Path file = Paths.get(filePath).normalize();
    java.nio.file.Path relative = base.relativize(file);
    for (Map.Entry<String, List<String>> entry : partitionInfo.entrySet()) {
      String key = entry.getKey();
      List<String> values = entry.getValue();
      for (String value : values) {
        String pathCheck = key + "=" + value;
        if (relative.startsWith(pathCheck)) {
          System.out.println("Relative " + relative + " " + pathCheck);
          partitionValues.add(
              PartitionValue.builder()
                  .partitionField(
                      InternalPartitionField.builder()
                          .sourceField(SchemaFieldFinder.getInstance().findFieldByPath(schema, key))
                          .transformType(PartitionTransformType.VALUE)
                          .build())
                  .range(Range.scalar(value))
                  .build());
        }
      }
    }
    return partitionValues;
  }*/
}
