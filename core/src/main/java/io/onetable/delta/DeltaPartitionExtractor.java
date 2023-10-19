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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

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
@Log4j2
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeltaPartitionExtractor {
  private static final DeltaPartitionExtractor INSTANCE = new DeltaPartitionExtractor();
  private static final String CAST_FUNCTION = "CAST(%s as DATE)";
  private static final String DATE_FORMAT_FUNCTION = "DATE_FORMAT(%s, '%s')";
  private static final String YEAR_FUNCTION = "YEAR(%s)";
  private static final String DATE_FORMAT_FOR_HOUR = "yyyy-MM-dd-HH";
  private static final String DATE_FORMAT_FOR_DAY = "yyyy-MM-dd";
  private static final String DATE_FORMAT_FOR_MONTH = "yyyy-MM";
  private static final String DATE_FORMAT_FOR_YEAR = "yyyy";
  // For timestamp partition fields, actual partition column names in delta format will be of type
  // generated & and with a name like `delta_partition_col_{transform_type}_{source_field_name}`.
  private static final String DELTA_PARTITION_COL_NAME_FORMAT = "onetable_partition_col_%s_%s";
  private static final String DELTA_GENERATION_EXPRESSION = "delta.generationExpression";
  private static final List<ParsedGeneratedExpr.GeneratedExprType> GRANULARITIES =
      Arrays.asList(
          ParsedGeneratedExpr.GeneratedExprType.YEAR,
          ParsedGeneratedExpr.GeneratedExprType.MONTH,
          ParsedGeneratedExpr.GeneratedExprType.DAY,
          ParsedGeneratedExpr.GeneratedExprType.HOUR);

  public static DeltaPartitionExtractor getInstance() {
    return INSTANCE;
  }

  /**
   * Extracts partition fields from delta table. Partitioning by nested columns isn't supported.
   * Example: Given a delta table and a reference to DeltaLog, method parameters can be obtained by
   * deltaLog = DeltaLog.forTable(spark, deltaTablePath); OneSchema oneSchema =
   * DeltaSchemaExtractor.getInstance().toOneSchema(deltaLog.snapshot().schema()); StructType
   * partitionSchema = deltaLog.metadata().partitionSchema();
   *
   * @param oneSchema canonical representation of the schema.
   * @param partitionSchema partition schema of the delta table.
   * @return list of canonical representation of the partition fields
   */
  public List<OnePartitionField> convertFromDeltaPartitionFormat(
      OneSchema oneSchema, StructType partitionSchema) {
    if (partitionSchema.isEmpty()) {
      return Collections.emptyList();
    }
    return getOnePartitionFields(partitionSchema, oneSchema);
  }

  /**
   * If all of them are value process individually and return. If they contain month they should
   * contain year as well. If they contain day they should contain month and year as well. If they
   * contain hour they should contain day, month and year as well. Other supports CAST(col as DATE)
   * and DATE_FORMAT(col, 'yyyy-MM-dd'). Partition by nested fields may not be fully supported.
   */
  private List<OnePartitionField> getOnePartitionFields(
      StructType partitionSchema, OneSchema oneSchema) {
    PeekingIterator<StructField> itr =
        Iterators.peekingIterator(Arrays.stream(partitionSchema.fields()).iterator());
    List<OnePartitionField> partitionFields = new ArrayList<>();
    while (itr.hasNext()) {
      StructField currPartitionField = itr.peek();
      if (!currPartitionField.metadata().contains(DELTA_GENERATION_EXPRESSION)) {
        partitionFields.add(
            OnePartitionField.builder()
                .sourceField(
                    SchemaFieldFinder.getInstance()
                        .findFieldByPath(oneSchema, currPartitionField.name()))
                .transformType(PartitionTransformType.VALUE)
                .build());
        itr.next(); // consume the field.
      } else {
        // Partition contains generated expression.
        // if it starts with year we should consume until we hit field with no generated expression
        // or we hit a field with generated expression that is of cast or date format.
        String expr = currPartitionField.metadata().getString(DELTA_GENERATION_EXPRESSION);
        ParsedGeneratedExpr parsedGeneratedExpr = ParsedGeneratedExpr.buildFromString(expr);
        if (ParsedGeneratedExpr.GeneratedExprType.CAST == parsedGeneratedExpr.generatedExprType) {
          partitionFields.add(getPartitionWithDateTransform(parsedGeneratedExpr, oneSchema));
          itr.next(); // consume the field.
        } else if (ParsedGeneratedExpr.GeneratedExprType.DATE_FORMAT
            == parsedGeneratedExpr.generatedExprType) {
          partitionFields.add(getPartitionWithDateFormatTransform(parsedGeneratedExpr, oneSchema));
          itr.next(); // consume the field.
        } else {
          // consume until we hit field with no generated expression or genearated expression
          // that is not of type cast or date format.
          List<ParsedGeneratedExpr> parsedGeneratedExprs = new ArrayList<>();
          while (itr.hasNext()
              && currPartitionField.metadata().contains(DELTA_GENERATION_EXPRESSION)) {
            expr = currPartitionField.metadata().getString(DELTA_GENERATION_EXPRESSION);
            parsedGeneratedExpr = ParsedGeneratedExpr.buildFromString(expr);

            if (ParsedGeneratedExpr.GeneratedExprType.CAST == parsedGeneratedExpr.generatedExprType
                || ParsedGeneratedExpr.GeneratedExprType.DATE_FORMAT
                    == parsedGeneratedExpr.generatedExprType) {
              break;
            }
            parsedGeneratedExprs.add(parsedGeneratedExpr);
            itr.next(); // consume the field
            if (itr.hasNext()) {
              currPartitionField = itr.peek();
            }
          }
          partitionFields.add(
              getPartitionColumnsForHourOrDayOrMonthOrYear(parsedGeneratedExprs, oneSchema));
        }
      }
    }
    return partitionFields;
  }

  private OnePartitionField getPartitionColumnsForHourOrDayOrMonthOrYear(
      List<ParsedGeneratedExpr> parsedGeneratedExprs, OneSchema oneSchema) {
    if (parsedGeneratedExprs.size() > 4) {
      throw new IllegalStateException("Invalid partition transform");
    }
    validate(
        parsedGeneratedExprs, new HashSet<>(GRANULARITIES.subList(0, parsedGeneratedExprs.size())));

    ParsedGeneratedExpr transform = parsedGeneratedExprs.get(0);
    return OnePartitionField.builder()
        .sourceField(
            SchemaFieldFinder.getInstance().findFieldByPath(oneSchema, transform.sourceColumn))
        .transformType(
            parsedGeneratedExprs.get(parsedGeneratedExprs.size() - 1)
                .internalPartitionTransformType)
        .build();
  }

  // Cast has default format of yyyy-MM-dd.
  private OnePartitionField getPartitionWithDateTransform(
      ParsedGeneratedExpr parsedGeneratedExpr, OneSchema oneSchema) {
    return OnePartitionField.builder()
        .sourceField(
            SchemaFieldFinder.getInstance()
                .findFieldByPath(oneSchema, parsedGeneratedExpr.sourceColumn))
        .transformType(PartitionTransformType.DAY)
        .build();
  }

  private OnePartitionField getPartitionWithDateFormatTransform(
      ParsedGeneratedExpr parsedGeneratedExpr, OneSchema oneSchema) {
    return OnePartitionField.builder()
        .sourceField(
            SchemaFieldFinder.getInstance()
                .findFieldByPath(oneSchema, parsedGeneratedExpr.sourceColumn))
        .transformType(parsedGeneratedExpr.internalPartitionTransformType)
        .build();
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
        return DATE_FORMAT_FOR_YEAR;
      case MONTH:
        return DATE_FORMAT_FOR_MONTH;
      case DAY:
        return DATE_FORMAT_FOR_DAY;
      case HOUR:
        return DATE_FORMAT_FOR_HOUR;
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

  private void validate(
      List<ParsedGeneratedExpr> parsedGeneratedExprs,
      Set<ParsedGeneratedExpr.GeneratedExprType> expectedTypesToBePresent) {
    Set<String> sourceFields =
        parsedGeneratedExprs.stream().map(expr -> expr.sourceColumn).collect(Collectors.toSet());
    if (sourceFields.size() > 1) {
      log.error(
          String.format("Multiple source columns found for partition transform: %s", sourceFields));
      throw new PartitionSpecException(
          String.format("Multiple source columns found for partition transform: %s", sourceFields));
    }
    Set<ParsedGeneratedExpr.GeneratedExprType> actualTypesPresent =
        parsedGeneratedExprs.stream()
            .map(expr -> expr.generatedExprType)
            .collect(Collectors.toSet());
    if (!actualTypesPresent.equals(expectedTypesToBePresent)) {
      log.error(
          "Mismatched types present. Expected: "
              + expectedTypesToBePresent
              + ", Found: "
              + actualTypesPresent);
      throw new PartitionSpecException(
          "Mismatched types present. Expected: "
              + expectedTypesToBePresent
              + ", Found: "
              + actualTypesPresent);
    }
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
    PartitionTransformType internalPartitionTransformType;

    private static ParsedGeneratedExpr buildFromString(String expr) {
      if (expr.contains("YEAR")) {
        return ParsedGeneratedExpr.builder()
            .generatedExprType(GeneratedExprType.YEAR)
            .sourceColumn(extractColumnName(expr, YEAR_PATTERN))
            .internalPartitionTransformType(PartitionTransformType.YEAR)
            .build();
      } else if (expr.contains("MONTH")) {
        return ParsedGeneratedExpr.builder()
            .generatedExprType(GeneratedExprType.MONTH)
            .sourceColumn(extractColumnName(expr, MONTH_PATTERN))
            .internalPartitionTransformType(PartitionTransformType.MONTH)
            .build();
      } else if (expr.contains("DAY")) {
        return ParsedGeneratedExpr.builder()
            .generatedExprType(GeneratedExprType.DAY)
            .sourceColumn(extractColumnName(expr, DAY_PATTERN))
            .internalPartitionTransformType(PartitionTransformType.DAY)
            .build();
      } else if (expr.contains("HOUR")) {
        return ParsedGeneratedExpr.builder()
            .generatedExprType(GeneratedExprType.HOUR)
            .sourceColumn(extractColumnName(expr, HOUR_PATTERN))
            .internalPartitionTransformType(PartitionTransformType.HOUR)
            .build();
      } else if (expr.contains("CAST")) {
        return ParsedGeneratedExpr.builder()
            .generatedExprType(GeneratedExprType.CAST)
            .sourceColumn(extractColumnName(expr, CAST_PATTERN))
            .internalPartitionTransformType(PartitionTransformType.DAY)
            .build();
      } else if (expr.contains("DATE_FORMAT")) {
        if (expr.startsWith("DATE_FORMAT(") && expr.endsWith(")")) {
          int firstParenthesisPos = expr.indexOf("(");
          int commaPos = expr.indexOf(",");
          int lastParenthesisPos = expr.lastIndexOf(")");
          /*
           * from DATE_FORMAT(source_col, 'yyyy-MM-dd-HH') the code below extracts yyyy-MM-dd-HH.
           */
          String dateFormatExpr =
              expr.substring(commaPos + 1, lastParenthesisPos).trim().replaceAll("^'|'$", "");
          return ParsedGeneratedExpr.builder()
              .generatedExprType(GeneratedExprType.DATE_FORMAT)
              .sourceColumn(expr.substring(firstParenthesisPos + 1, commaPos).trim())
              .internalPartitionTransformType(computeInternalPartitionTransform(dateFormatExpr))
              .build();
        } else {
          throw new IllegalArgumentException("Could not extract values from: " + expr);
        }
      } else {
        throw new IllegalArgumentException(
            "Unsupported expression for generated expression: " + expr);
      }
    }

    // Supporting granularity as per https://docs.databricks.com/en/delta/generated-columns.html
    private static PartitionTransformType computeInternalPartitionTransform(String dateFormatExpr) {
      if (DATE_FORMAT_FOR_HOUR.equals(dateFormatExpr)) {
        return PartitionTransformType.HOUR;
      } else if (DATE_FORMAT_FOR_DAY.equals(dateFormatExpr)) {
        return PartitionTransformType.DAY;
      } else if (DATE_FORMAT_FOR_MONTH.equals(dateFormatExpr)) {
        return PartitionTransformType.MONTH;
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Unsupported date format expression: %s for generated expression", dateFormatExpr));
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
