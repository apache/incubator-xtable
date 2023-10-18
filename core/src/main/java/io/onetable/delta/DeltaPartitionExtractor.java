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

import static io.onetable.delta.DeltaValueSerializer.getFormattedValueForPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import io.onetable.exception.PartitionSpecException;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.stat.Range;
import io.onetable.model.storage.OneDataFile;
import io.onetable.schema.SchemaFieldFinder;

/**
 * DeltaPartitionExtractor handles extracting partition columns, also creating generated columns in
 * the certain cases. It is also responsible for PartitionValue Serialization leveraging {@link
 * DeltaValueSerializer}.
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

  /**
   * Extracts partition fields from delta table. Example: Given a delta table and a reference to
   * DeltaLog, method parameters can be obtained by deltaLog = DeltaLog.forTable(spark,
   * deltaTablePath); StructType tableSchema = deltaLog.snapshot().schema(); List<String>
   * partitionFields = JavaConverters.seqAsJavaList(deltaLog.metadata().partitionColumns());
   *
   * @param tableSchema schema of the delta table.
   * @param oneSchema canonical representation of the schema.
   * @param partitionColumns partition columns of the delta table.
   * @return list of canonical representation of the partition fields
   */
  public List<OnePartitionField> convertFromDeltaPartitionFormat(
      StructType tableSchema, OneSchema oneSchema, List<String> partitionColumns) {
    if (partitionColumns == null || partitionColumns.isEmpty()) {
      return Collections.emptyList();
    }
    Map<String, StructField> partitionColToStructFieldMap =
        partitionColumns.stream()
            .collect(
                Collectors.toMap(
                    partitionCol -> partitionCol,
                    partitionCol -> findFieldByPath(tableSchema, partitionCol)));
    List<String> partitionColsNotFoundInDelta =
        partitionColToStructFieldMap.entrySet().stream()
            .filter(entry -> entry.getValue() == null)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    // Even generated columns should be present in the schema.
    if (!partitionColsNotFoundInDelta.isEmpty()) {
      throw new PartitionSpecException(
          String.format("Partition columns not found in schema: %s", partitionColsNotFoundInDelta));
    }
    List<String> partitionColumnsNotFoundInCanonical =
        partitionColToStructFieldMap.entrySet().stream()
            .filter(entry -> entry.getValue() == null)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    // This check is a more of defensive one as it is unlikely to happen and should be caught by
    // schema extractor in most cases.
    if (!partitionColumnsNotFoundInCanonical.isEmpty()) {
      throw new PartitionSpecException(
          String.format(
              "Partition columns not found in canonical schema: %s",
              partitionColumnsNotFoundInCanonical));
    }
    return getOnePartitionFields(partitionColToStructFieldMap, oneSchema);
  }

  // If all of them are value process individually and return.
  // If they contain month they should contain year as well.
  // If they contain day they should contain month and year as well.
  // If they contain hour they should contain day, month and year as well.
  // The above are not enforced as these are standard and assumed. We can enforce if need be.
  // Other supports CAST(col as DATE) and DATE_FORMAT(col, 'yyyy-MM-dd')
  // Partition by nested fields may not be fully supported.
  private List<OnePartitionField> getOnePartitionFields(
      Map<String, StructField> partitionColToStructFieldMap, OneSchema oneSchema) {
    // TODO(vamshigv): Order is not preserved.
    List<String> partitionColumnsWithoutGeneratedExpr =
        partitionColToStructFieldMap.entrySet().stream()
            .filter(entry -> !entry.getValue().metadata().contains(DELTA_GENERATION_EXPRESSION))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    List<OnePartitionField> valueTransformPartitionFields =
        partitionColumnsWithoutGeneratedExpr.stream()
            .map(
                partitionCol ->
                    OnePartitionField.builder()
                        .sourceField(
                            SchemaFieldFinder.getInstance()
                                .findFieldByPath(oneSchema, partitionCol))
                        .transformType(PartitionTransformType.VALUE)
                        .build())
            .collect(Collectors.toList());
    List<String> partitionColumnsWithGeneratedExpr =
        partitionColToStructFieldMap.entrySet().stream()
            .filter(entry -> entry.getValue().metadata().contains(DELTA_GENERATION_EXPRESSION))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    valueTransformPartitionFields.addAll(
        getPartitionColumnsForGeneratedExpr(
            partitionColumnsWithGeneratedExpr, partitionColToStructFieldMap, oneSchema));
    return valueTransformPartitionFields;
  }

  private List<OnePartitionField> getPartitionColumnsForGeneratedExpr(
      List<String> partitionColumnsWithGeneratedExpr,
      Map<String, StructField> partitionColToStructFieldMap,
      OneSchema oneSchema) {
    List<OnePartitionField> partitionFields = new ArrayList<>();
    List<ParsedGeneratedExpr> parsedGeneratedExprs =
        partitionColumnsWithGeneratedExpr.stream()
            .map(
                columnName -> {
                  String expr =
                      partitionColToStructFieldMap
                          .get(columnName)
                          .metadata()
                          .getString(DELTA_GENERATION_EXPRESSION);
                  return ParsedGeneratedExpr.buildFromString(expr);
                })
            .collect(Collectors.toList());
    // Ordering of the operation here is important.
    OnePartitionField hourOrDayorMonthOrYearPartitionField =
        getPartitionWithHourTransform(parsedGeneratedExprs, oneSchema)
            .orElseGet(
                () ->
                    getPartitionWithDayTransform(parsedGeneratedExprs, oneSchema)
                        .orElseGet(
                            () ->
                                getPartitionWithMonthTransform(parsedGeneratedExprs, oneSchema)
                                    .orElseGet(
                                        () ->
                                            getPartitionWithYearTransform(
                                                    parsedGeneratedExprs, oneSchema)
                                                .orElse(null))));
    if (hourOrDayorMonthOrYearPartitionField != null) {
      partitionFields.add(hourOrDayorMonthOrYearPartitionField);
    }
    // If there are any other generated expressions, they should be of type date or date format.
    parsedGeneratedExprs.stream()
        .map(
            parsedGeneratedExpr -> {
              if (parsedGeneratedExpr.generatedExprType
                  == ParsedGeneratedExpr.GeneratedExprType.CAST) {
                return getPartitionWithDateTransform(parsedGeneratedExpr, oneSchema);
              } else if (parsedGeneratedExpr.generatedExprType
                  == ParsedGeneratedExpr.GeneratedExprType.DATE_FORMAT) {
                return getPartitionWithDateFormatTransform(parsedGeneratedExpr, oneSchema);
              } else {
                return null;
              }
            })
        .filter(Objects::nonNull)
        .forEach(partitionFields::add);
    return partitionFields;
  }

  // Cast has default format of yyyy-MM-dd.
  private OnePartitionField getPartitionWithDateTransform(
      ParsedGeneratedExpr parsedGeneratedExpr, OneSchema oneSchema) {
    // TODO(vamshigv): Check if we can be more defensive here.
    return OnePartitionField.builder()
        .sourceField(
            SchemaFieldFinder.getInstance()
                .findFieldByPath(oneSchema, parsedGeneratedExpr.sourceColumn))
        .transformType(PartitionTransformType.DAY)
        .build();
  }

  // TODO(vamshigv): Transform type should depend on the date format parsed.
  private OnePartitionField getPartitionWithDateFormatTransform(
      ParsedGeneratedExpr parsedGeneratedExpr, OneSchema oneSchema) {
    // TODO(vamshigv): Check if we can be more defensive here.
    return OnePartitionField.builder()
        .sourceField(
            SchemaFieldFinder.getInstance()
                .findFieldByPath(oneSchema, parsedGeneratedExpr.sourceColumn))
        .transformType(PartitionTransformType.DAY)
        .build();
  }

  private Optional<OnePartitionField> getPartitionWithHourTransform(
      List<ParsedGeneratedExpr> parsedGeneratedExprs, OneSchema oneSchema) {
    // return non-empty if contains a field with hour transform in generated expression.
    // parse the field name and find more generated expressions for day, month and year or fail if
    // not found.
    if (parsedGeneratedExprs.size() < 4) {
      return Optional.empty();
    }
    List<ParsedGeneratedExpr> hourTransforms =
        parsedGeneratedExprs.stream()
            .filter(expr -> expr.generatedExprType == ParsedGeneratedExpr.GeneratedExprType.HOUR)
            .collect(Collectors.toList());

    if (hourTransforms.isEmpty()) {
      return Optional.empty();
    }

    if (hourTransforms.size() > 1) {
      throw new PartitionSpecException(
          "Multiple hour transforms found and currently not supported");
    }
    ParsedGeneratedExpr hourTransform = hourTransforms.get(0);

    List<ParsedGeneratedExpr> dayTransforms =
        parsedGeneratedExprs.stream()
            .filter(expr -> expr.generatedExprType == ParsedGeneratedExpr.GeneratedExprType.DAY)
            .collect(Collectors.toList());
    if (dayTransforms.isEmpty()
        || dayTransforms.size() > 1
        || !dayTransforms.get(0).sourceColumn.equals(hourTransform.sourceColumn)) {
      throw new PartitionSpecException(
          "Day transform not found or multiple day transforms found or day transform not matching with hour transform");
    }

    List<ParsedGeneratedExpr> monthTransforms =
        parsedGeneratedExprs.stream()
            .filter(expr -> expr.generatedExprType == ParsedGeneratedExpr.GeneratedExprType.MONTH)
            .collect(Collectors.toList());
    if (monthTransforms.isEmpty()
        || monthTransforms.size() > 1
        || !monthTransforms.get(0).sourceColumn.equals(hourTransform.sourceColumn)) {
      throw new PartitionSpecException(
          "Month transform not found or multiple month transforms found or month transform not matching with hour transform");
    }

    List<ParsedGeneratedExpr> yearTransforms =
        parsedGeneratedExprs.stream()
            .filter(expr -> expr.generatedExprType == ParsedGeneratedExpr.GeneratedExprType.YEAR)
            .collect(Collectors.toList());
    if (yearTransforms.isEmpty()
        || yearTransforms.size() > 1
        || !yearTransforms.get(0).sourceColumn.equals(hourTransform.sourceColumn)) {
      throw new PartitionSpecException(
          "Year transform not found or multiple year transforms found or year transform not matching with hour transform");
    }

    // TODO(vamshigv): Check if we can be more defensive here.
    return Optional.of(
        OnePartitionField.builder()
            .sourceField(
                SchemaFieldFinder.getInstance()
                    .findFieldByPath(oneSchema, hourTransform.sourceColumn))
            .transformType(PartitionTransformType.HOUR)
            .build());
  }

  private Optional<OnePartitionField> getPartitionWithDayTransform(
      List<ParsedGeneratedExpr> parsedGeneratedExprs, OneSchema oneSchema) {
    if (parsedGeneratedExprs.size() < 3) {
      return Optional.empty();
    }
    List<ParsedGeneratedExpr> dayTransforms =
        parsedGeneratedExprs.stream()
            .filter(expr -> expr.generatedExprType == ParsedGeneratedExpr.GeneratedExprType.DAY)
            .collect(Collectors.toList());

    if (dayTransforms.isEmpty()) {
      return Optional.empty();
    }

    if (dayTransforms.size() > 1) {
      throw new PartitionSpecException("Multiple day transforms found and currently not supported");
    }
    ParsedGeneratedExpr dayTransform = dayTransforms.get(0);

    List<ParsedGeneratedExpr> monthTransforms =
        parsedGeneratedExprs.stream()
            .filter(expr -> expr.generatedExprType == ParsedGeneratedExpr.GeneratedExprType.MONTH)
            .collect(Collectors.toList());
    if (monthTransforms.isEmpty()
        || monthTransforms.size() > 1
        || !monthTransforms.get(0).sourceColumn.equals(dayTransform.sourceColumn)) {
      throw new PartitionSpecException(
          "Month transform not found or multiple month transforms found or month transform not matching with day transform");
    }

    List<ParsedGeneratedExpr> yearTransforms =
        parsedGeneratedExprs.stream()
            .filter(expr -> expr.generatedExprType == ParsedGeneratedExpr.GeneratedExprType.YEAR)
            .collect(Collectors.toList());
    if (yearTransforms.isEmpty()
        || yearTransforms.size() > 1
        || !yearTransforms.get(0).sourceColumn.equals(dayTransform.sourceColumn)) {
      throw new PartitionSpecException(
          "Year transform not found or multiple year transforms found or year transform not matching with day transform");
    }

    // TODO(vamshigv): Check if we can be more defensive here.
    return Optional.of(
        OnePartitionField.builder()
            .sourceField(
                SchemaFieldFinder.getInstance()
                    .findFieldByPath(oneSchema, dayTransform.sourceColumn))
            .transformType(PartitionTransformType.DAY)
            .build());
  }

  private Optional<OnePartitionField> getPartitionWithMonthTransform(
      List<ParsedGeneratedExpr> parsedGeneratedExprs, OneSchema oneSchema) {
    if (parsedGeneratedExprs.size() < 2) {
      return Optional.empty();
    }
    List<ParsedGeneratedExpr> monthTransforms =
        parsedGeneratedExprs.stream()
            .filter(expr -> expr.generatedExprType == ParsedGeneratedExpr.GeneratedExprType.MONTH)
            .collect(Collectors.toList());

    if (monthTransforms.isEmpty()) {
      return Optional.empty();
    }

    if (monthTransforms.size() > 1) {
      throw new PartitionSpecException(
          "Multiple month transforms found and currently not supported");
    }
    ParsedGeneratedExpr monthTransform = monthTransforms.get(0);

    List<ParsedGeneratedExpr> yearTransforms =
        parsedGeneratedExprs.stream()
            .filter(expr -> expr.generatedExprType == ParsedGeneratedExpr.GeneratedExprType.YEAR)
            .collect(Collectors.toList());
    if (yearTransforms.isEmpty()
        || yearTransforms.size() > 1
        || !yearTransforms.get(0).sourceColumn.equals(monthTransform.sourceColumn)) {
      throw new PartitionSpecException(
          "Year transform not found or multiple year transforms found or year transform not matching with month transform");
    }

    // TODO(vamshigv): Check if we can be more defensive here.
    return Optional.of(
        OnePartitionField.builder()
            .sourceField(
                SchemaFieldFinder.getInstance()
                    .findFieldByPath(oneSchema, monthTransform.sourceColumn))
            .transformType(PartitionTransformType.MONTH)
            .build());
  }

  private Optional<OnePartitionField> getPartitionWithYearTransform(
      List<ParsedGeneratedExpr> parsedGeneratedExprs, OneSchema oneSchema) {
    if (parsedGeneratedExprs.size() < 1) {
      return Optional.empty();
    }
    List<ParsedGeneratedExpr> yearTransforms =
        parsedGeneratedExprs.stream()
            .filter(expr -> expr.generatedExprType == ParsedGeneratedExpr.GeneratedExprType.YEAR)
            .collect(Collectors.toList());

    if (yearTransforms.isEmpty()) {
      return Optional.empty();
    }

    if (yearTransforms.size() > 1) {
      throw new PartitionSpecException(
          "Multiple year transforms found and currently not supported");
    }
    ParsedGeneratedExpr yearTransform = yearTransforms.get(0);

    // TODO(vamshigv): Check if we can be more defensive here.
    return Optional.of(
        OnePartitionField.builder()
            .sourceField(
                SchemaFieldFinder.getInstance()
                    .findFieldByPath(oneSchema, yearTransform.sourceColumn))
            .transformType(PartitionTransformType.YEAR)
            .build());
  }

  public Map<String, StructField> convertToDeltaPartitionFormat(
      List<OnePartitionField> partitionFields) {
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
            getFormattedValueForPartition(
                e.getValue().getMaxValue(),
                e.getKey().getSourceField().getSchema().getDataType(),
                transformType,
                "");
        partitionValuesSerialized.put(
            e.getKey().getSourceField().getName(), partitionValueSerialized);
      } else {
        // use appropriate date formatter for value serialization.
        partitionValueSerialized =
            getFormattedValueForPartition(
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

  // Find the field in the structType by path where path can be nested with dot notation like a.b.c
  private StructField findFieldByPath(StructType structType, String path) {
    if (path == null || path.isEmpty()) {
      return null;
    }
    StructType currStructType = structType;
    String[] pathParts = path.split("\\.");
    for (int i = 0; i < pathParts.length; i++) {
      StructField[] currFields = currStructType.fields();
      int lookupIndex = currStructType.fieldIndex(pathParts[i]);
      if (lookupIndex < 0 || lookupIndex >= currFields.length) {
        return null;
      }
      StructField currField = currFields[lookupIndex];
      if (i == pathParts.length - 1) {
        return currField;
      }
      if (!(currField.dataType() instanceof StructType)) {
        return null;
      }
      currStructType = (StructType) currField.dataType();
    }
    return null;
  }

  private PartitionTransformType getTransformType(StructField partitionColStructField) {
    if (!partitionColStructField.metadata().contains(DELTA_GENERATION_EXPRESSION)) {
      return PartitionTransformType.VALUE;
    }
    String generatedExprCol =
        partitionColStructField.metadata().getString(DELTA_GENERATION_EXPRESSION);
    // Refer https://docs.databricks.com/en/delta/generated-columns.html
    // TODO(vamshigv): This is Rudimentary check, improve it
    // The below logic needs correction
    if (generatedExprCol.contains("YEAR")) {
      return PartitionTransformType.YEAR;
    } else if (generatedExprCol.contains("MONTH")) {
      return PartitionTransformType.MONTH;
    } else if (generatedExprCol.contains("DAY")) {
      return PartitionTransformType.DAY;
    } else if (generatedExprCol.contains("HOUR")) {
      return PartitionTransformType.HOUR;
    } else if (generatedExprCol.contains("CAST")) {
      return PartitionTransformType.DAY;
    } else if (generatedExprCol.contains("DATE_FORMAT")) {
      return PartitionTransformType.DAY;
    }
    throw new PartitionSpecException(
        String.format("Unsupported generated expression: %s", generatedExprCol));
  }

  @Builder
  static class ParsedGeneratedExpr {

    private static final Pattern YEAR_PATTERN = Pattern.compile("YEAR\\(([^)]+)\\)");
    private static final Pattern MONTH_PATTERN = Pattern.compile("MONTH\\(([^)]+)\\)");
    private static final Pattern DAY_PATTERN = Pattern.compile("DAY\\(([^)]+)\\)");
    private static final Pattern HOUR_PATTERN = Pattern.compile("HOUR\\(([^)]+)\\)");
    private static final Pattern CAST_PATTERN = Pattern.compile("CAST\\(([^ ]+) AS DATE\\)");

    enum GeneratedExprType {
      YEAR,
      MONTH,
      DAY,
      HOUR,
      CAST,
      DATE_FORMAT
    }

    String sourceColumn;
    GeneratedExprType generatedExprType;
    String dateFormat;

    public static ParsedGeneratedExpr buildFromString(String expr) {
      if (expr.contains("YEAR")) {
        return ParsedGeneratedExpr.builder()
            .generatedExprType(GeneratedExprType.YEAR)
            .sourceColumn(extractColumnName(expr, YEAR_PATTERN))
            .build();
      } else if (expr.contains("MONTH")) {
        return ParsedGeneratedExpr.builder()
            .generatedExprType(GeneratedExprType.MONTH)
            .sourceColumn(extractColumnName(expr, MONTH_PATTERN))
            .build();
      } else if (expr.contains("DAY")) {
        return ParsedGeneratedExpr.builder()
            .generatedExprType(GeneratedExprType.DAY)
            .sourceColumn(extractColumnName(expr, DAY_PATTERN))
            .build();
      } else if (expr.contains("HOUR")) {
        return ParsedGeneratedExpr.builder()
            .generatedExprType(GeneratedExprType.HOUR)
            .sourceColumn(extractColumnName(expr, HOUR_PATTERN))
            .build();
      } else if (expr.contains("CAST")) {
        return ParsedGeneratedExpr.builder()
            .generatedExprType(GeneratedExprType.CAST)
            .sourceColumn(extractColumnName(expr, CAST_PATTERN))
            .build();
      } else if (expr.contains("DATE_FORMAT")) {
        // TODO(vamshigv): Better this.
        if (expr.startsWith("DATE_FORMAT(") && expr.endsWith(")")) {
          int firstParenthesisPos = expr.indexOf("(");
          int commaPos = expr.indexOf(",");
          int lastParenthesisPos = expr.lastIndexOf(")");
          return ParsedGeneratedExpr.builder()
              .generatedExprType(GeneratedExprType.DATE_FORMAT)
              .sourceColumn(expr.substring(firstParenthesisPos + 1, commaPos).trim())
              .dateFormat(
                  expr.substring(commaPos + 1, lastParenthesisPos).trim().replaceAll("^'|'$", ""))
              .build();
        } else {
          throw new IllegalArgumentException("Could not extract values from: " + expr);
        }
      } else {
        throw new IllegalArgumentException(
            "Unsupported expression for generated expression: " + expr);
      }
    }

    private static String extractColumnName(String expr, Pattern regexPattern) {
      Matcher matcher = regexPattern.matcher(expr);
      if (matcher.find()) {
        return matcher.group(1).trim();
      }
      throw new IllegalArgumentException(
          "Could not extract column name from: "
              + expr
              + " using pattern: "
              + regexPattern.pattern());
    }
  }
}
