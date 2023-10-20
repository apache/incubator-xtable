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
 
package io.onetable.delta;

import static io.onetable.delta.DeltaValueConverter.convertToDeltaPartitionValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

import io.onetable.exception.PartitionSpecException;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.stat.Range;
import io.onetable.model.storage.OneDataFile;

/**
 * DeltaPartitionExtractor handles extracting partition columns, also creating generated columns in
 * the certain cases. It is also responsible for PartitionValue Serialization leveraging {@link
 * DeltaValueConverter}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeltaPartitionExtractor {
  private static final DeltaPartitionExtractor INSTANCE = new DeltaPartitionExtractor();
  private static final String CAST_FUNCTION = "CAST(%s as DATE)";
  private static final String DATE_FORMAT_FUNCTION = "DATE_FORMAT(%s, '%s')";
  private static final String YEAR_FUNCTION = "YEAR(%s)";
  // For timestamp partition fields, actual partition column names in delta format will be of type
  // generated & and with a name like `delta_partition_col_{transform_type}_{source_field_name}`.
  private static final String DELTA_PARTITION_COL_NAME_FORMAT = "onetable_partition_col_%s_%s";
  private static final String DELTA_GENERATION_EXPRESSION = "delta.generationExpression";

  public static DeltaPartitionExtractor getInstance() {
    return INSTANCE;
  }

  public Map<String, StructField> getPartitionColumns(List<OnePartitionField> partitionFields) {
    if (partitionFields == null) {
      return null;
    }
    Map<String, StructField> nameToStructFieldMap = new HashMap<>();
    for (OnePartitionField onePartitionField : partitionFields) {
      String currPartitionColumnName;
      StructField field;

      if (onePartitionField.getTransformType() == PartitionTransformType.VALUE) {
        currPartitionColumnName = onePartitionField.getSourceField().getName();
        field = null;
      } else {
        // Since partition field of timestamp type, create new field in schema.
        field = getGeneratedField(onePartitionField);
        currPartitionColumnName = field.name();
      }
      nameToStructFieldMap.put(currPartitionColumnName, field);
    }
    return nameToStructFieldMap;
  }

  public Map<String, String> partitionValueSerialization(OneDataFile oneDataFile) {
    Map<String, String> partitionValuesSerialized = new HashMap<>();
    if (oneDataFile.getPartitionValues() == null || oneDataFile.getPartitionValues().isEmpty()) {
      return partitionValuesSerialized;
    }
    for (Map.Entry<OnePartitionField, Range> e : oneDataFile.getPartitionValues().entrySet()) {
      PartitionTransformType transformType = e.getKey().getTransformType();
      String partitionValueSerialized;
      if (transformType == PartitionTransformType.VALUE) {
        partitionValueSerialized =
            convertToDeltaPartitionValue(
                e.getValue().getMaxValue(),
                e.getKey().getSourceField().getSchema().getDataType(),
                transformType,
                "");
        partitionValuesSerialized.put(
            e.getKey().getSourceField().getName(), partitionValueSerialized);
      } else {
        // use appropriate date formatter for value serialization.
        partitionValueSerialized =
            convertToDeltaPartitionValue(
                e.getValue().getMaxValue(),
                e.getKey().getSourceField().getSchema().getDataType(),
                transformType,
                getDateFormat(e.getKey().getTransformType()));
        partitionValuesSerialized.put(getGeneratedColumnName(e.getKey()), partitionValueSerialized);
      }
    }
    return partitionValuesSerialized;
  }

  private String getGeneratedColumnName(OnePartitionField onePartitionField) {
    return String.format(
        DELTA_PARTITION_COL_NAME_FORMAT,
        onePartitionField.getTransformType().toString(),
        onePartitionField.getSourceField().getName());
  }

  private String getDateFormat(PartitionTransformType transformType) {
    switch (transformType) {
      case YEAR:
        return "yyyy";
      case MONTH:
        return "yyyy-MM";
      case DAY:
        return "yyyy-MM-dd";
      case HOUR:
        return "yyyy-MM-dd-HH";
      default:
        throw new PartitionSpecException("Invalid transform type");
    }
  }

  private StructField getGeneratedField(OnePartitionField onePartitionField) {
    String generatedExpression;
    DataType dataType;
    String currPartitionColumnName = getGeneratedColumnName(onePartitionField);
    Map<String, String> generatedExpressionMetadata = new HashMap<>();
    switch (onePartitionField.getTransformType()) {
      case YEAR:
        generatedExpression =
            String.format(YEAR_FUNCTION, onePartitionField.getSourceField().getPath());
        dataType = DataTypes.IntegerType;
        break;
      case MONTH:
      case HOUR:
        generatedExpression =
            String.format(
                DATE_FORMAT_FUNCTION,
                onePartitionField.getSourceField().getPath(),
                getDateFormat(onePartitionField.getTransformType()));
        dataType = DataTypes.StringType;
        break;
      case DAY:
        generatedExpression =
            String.format(CAST_FUNCTION, onePartitionField.getSourceField().getPath());
        dataType = DataTypes.DateType;
        break;
      default:
        throw new PartitionSpecException("Invalid transform type");
    }
    generatedExpressionMetadata.put(DELTA_GENERATION_EXPRESSION, generatedExpression);
    Metadata partitionFieldMetadata =
        new Metadata(ScalaUtils.convertJavaMapToScala(generatedExpressionMetadata));
    return new StructField(currPartitionColumnName, dataType, true, partitionFieldMetadata);
  }
}
