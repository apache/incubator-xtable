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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Dataset$;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.EqualNullSafe;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.Assignment;
import org.apache.spark.sql.catalyst.plans.logical.InsertAction;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeAction;
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable;
import org.apache.spark.sql.catalyst.plans.logical.UpdateAction;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.functions;

import scala.Option;
import scala.collection.JavaConverters;

/**
 * Turns the row-level commands Spark resolved against a keyed Hudi-managed table into the rows Hudi
 * should upsert or delete. Everything is expressed with the Dataset API over the analyzed plans, so
 * the target is read through Iceberg (exactly as Iceberg's own row-level writes read it) and Hudi's
 * index does the file-group lookup on write.
 *
 * <p>Returns null when a statement has semantics Hudi cannot reproduce with one upsert or delete
 * (record-key or partition updates, delete actions inside MERGE, non-key merge conditions); the
 * caller then leaves the plan untouched.
 */
final class HudiRowLevelPlanner {
  private HudiRowLevelPlanner() {}

  /** Why a statement was not routed, for the warning log. */
  static final class Unsupported extends RuntimeException {
    Unsupported(String reason) {
      super(reason);
    }
  }

  static Function<Dataset<Row>, Dataset<Row>> update(
      SparkSession spark, DataSourceV2Relation target, UpdateTable update) {
    HudiTableContext context = ((HudiSparkTable) target.table()).context();
    List<Assignment> assignments = seq(update.assignments());
    List<Attribute> targetColumns = attributes(target.output());
    checkAssignmentsKeepKeysAndPartitions(
        assignments, targetColumns, context, Collections.emptyMap());
    return rows -> rows.select(projection(assignments, targetColumns));
  }

  static Function<Dataset<Row>, Dataset<Row>> merge(
      SparkSession spark, DataSourceV2Relation target, MergeIntoTable merge) {
    HudiTableContext context = ((HudiSparkTable) target.table()).context();
    List<Attribute> targetColumns = attributes(target.output());
    if (!seq(merge.notMatchedBySourceActions()).isEmpty()) {
      throw new Unsupported("WHEN NOT MATCHED BY SOURCE is not supported yet");
    }
    UpdateAction update = singleAction(seq(merge.matchedActions()), UpdateAction.class, "MATCHED");
    InsertAction insert =
        singleAction(seq(merge.notMatchedActions()), InsertAction.class, "NOT MATCHED");
    if (update == null && insert == null) {
      throw new Unsupported("MERGE without an UPDATE or INSERT action");
    }
    Map<String, Expression> keyValuesFromCondition =
        checkMergeConditionIsKeyEquality(
            merge.mergeCondition(), targetColumns, merge.sourceTable(), context);
    if (update != null) {
      // UPDATE SET * assigns t.key = s.key; that is a no-op under ON t.key = s.key
      checkAssignmentsKeepKeysAndPartitions(
          seq(update.assignments()), targetColumns, context, keyValuesFromCondition);
    }
    Column joinCondition = new Column(merge.mergeCondition());
    LogicalPlan targetPlan = target;
    return source -> {
      Dataset<Row> targetRows = Dataset$.MODULE$.ofRows(spark, targetPlan);
      Dataset<Row> result = null;
      if (update != null) {
        Dataset<Row> matched = targetRows.join(source, joinCondition, "inner");
        if (update.condition().isDefined()) {
          matched = matched.filter(new Column(update.condition().get()));
        }
        result = matched.select(projection(seq(update.assignments()), targetColumns));
      }
      if (insert != null) {
        Dataset<Row> unmatched = source.join(targetRows, joinCondition, "left_anti");
        if (insert.condition().isDefined()) {
          unmatched = unmatched.filter(new Column(insert.condition().get()));
        }
        Dataset<Row> inserts =
            unmatched.select(projection(seq(insert.assignments()), targetColumns, true));
        result = result == null ? inserts : result.union(inserts);
      }
      return result;
    };
  }

  /**
   * One column per target column, in target order. Assignments may or may not have been aligned by
   * Spark yet, so values are cast to the column type and unassigned columns fall back to the target
   * value (updates) or NULL (inserts).
   */
  private static Column[] projection(
      List<Assignment> assignments, List<Attribute> targetColumns, boolean missingAsNull) {
    Column[] columns = new Column[targetColumns.size()];
    for (int i = 0; i < targetColumns.size(); i++) {
      Attribute column = targetColumns.get(i);
      Column value =
          missingAsNull ? functions.lit(null).cast(column.dataType()) : new Column(column);
      for (Assignment assignment : assignments) {
        if (assignment.key() instanceof AttributeReference
            && ((AttributeReference) assignment.key()).exprId().equals(column.exprId())) {
          value = new Column(assignment.value()).cast(column.dataType());
          break;
        }
      }
      columns[i] = value.as(column.name());
    }
    return columns;
  }

  private static Column[] projection(List<Assignment> assignments, List<Attribute> targetColumns) {
    return projection(assignments, targetColumns, false);
  }

  private static void checkAssignmentsKeepKeysAndPartitions(
      List<Assignment> assignments,
      List<Attribute> targetColumns,
      HudiTableContext context,
      Map<String, Expression> equivalentKeyValues) {
    Set<String> immutable = new HashSet<>();
    for (String field : context.getRecordKeyFields()) {
      immutable.add(field.toLowerCase(Locale.ROOT));
    }
    for (String field : context.getPartitionFields()) {
      immutable.add(field.toLowerCase(Locale.ROOT));
    }
    for (Assignment assignment : assignments) {
      if (!(assignment.key() instanceof AttributeReference)) {
        throw new Unsupported("assignment to a nested field: " + assignment.key());
      }
      AttributeReference key = (AttributeReference) assignment.key();
      String name = key.name().toLowerCase(Locale.ROOT);
      Expression equivalent = equivalentKeyValues.get(name);
      if (immutable.contains(name)
          && !assignment.value().semanticEquals(key)
          && (equivalent == null || !assignment.value().semanticEquals(equivalent))) {
        throw new Unsupported(
            "assignment changes record key or partition column "
                + key.name()
                + "; Hudi upserts cannot move a record between keys or partitions");
      }
    }
  }

  /**
   * {@code t.k1 = s.x AND t.k2 = s.y}: every record key column once, nothing else. Returns the
   * source-side expression each key is equated with.
   */
  private static Map<String, Expression> checkMergeConditionIsKeyEquality(
      Expression condition,
      List<Attribute> targetColumns,
      LogicalPlan source,
      HudiTableContext context) {
    Set<String> keyColumns = new HashSet<>();
    for (String field : context.getRecordKeyFields()) {
      keyColumns.add(field.toLowerCase(Locale.ROOT));
    }
    Map<String, Expression> matched = new HashMap<>();
    for (Expression conjunct : conjuncts(condition)) {
      Expression left;
      Expression right;
      if (conjunct instanceof EqualTo) {
        left = ((EqualTo) conjunct).left();
        right = ((EqualTo) conjunct).right();
      } else if (conjunct instanceof EqualNullSafe) {
        left = ((EqualNullSafe) conjunct).left();
        right = ((EqualNullSafe) conjunct).right();
      } else {
        throw new Unsupported("merge condition is not a key equality: " + conjunct);
      }
      AttributeReference targetKey = targetKey(left, targetColumns, keyColumns);
      Expression sourceSide = right;
      if (targetKey == null) {
        targetKey = targetKey(right, targetColumns, keyColumns);
        sourceSide = left;
      }
      if (targetKey == null || !referencesOnly(sourceSide, source)) {
        throw new Unsupported("merge condition is not a key equality: " + conjunct);
      }
      matched.put(targetKey.name().toLowerCase(Locale.ROOT), sourceSide);
    }
    if (!matched.keySet().equals(keyColumns)) {
      throw new Unsupported(
          "merge condition must equate every record key column " + context.getRecordKeyFields());
    }
    return matched;
  }

  private static AttributeReference targetKey(
      Expression expr, List<Attribute> targetColumns, Set<String> keyColumns) {
    if (!(expr instanceof AttributeReference)) {
      return null;
    }
    AttributeReference attr = (AttributeReference) expr;
    for (Attribute column : targetColumns) {
      if (column.exprId().equals(attr.exprId())
          && keyColumns.contains(column.name().toLowerCase(Locale.ROOT))) {
        return attr;
      }
    }
    return null;
  }

  private static boolean referencesOnly(Expression expr, LogicalPlan plan) {
    Set<Object> ids = new HashSet<>();
    for (Attribute attr : attributes(plan.output())) {
      ids.add(attr.exprId());
    }
    for (Attribute attr : attributes(expr.references().toSeq())) {
      if (!ids.contains(attr.exprId())) {
        return false;
      }
    }
    return true;
  }

  private static List<Expression> conjuncts(Expression expr) {
    List<Expression> out = new ArrayList<>();
    if (expr instanceof And) {
      out.addAll(conjuncts(((And) expr).left()));
      out.addAll(conjuncts(((And) expr).right()));
    } else {
      out.add(expr);
    }
    return out;
  }

  @SuppressWarnings("unchecked")
  private static <T extends MergeAction> T singleAction(
      List<MergeAction> actions, Class<T> type, String clause) {
    if (actions.isEmpty()) {
      return null;
    }
    if (actions.size() > 1 || !type.isInstance(actions.get(0))) {
      throw new Unsupported(
          "only a single WHEN "
              + clause
              + " THEN "
              + type.getSimpleName().replace("Action", "").toUpperCase(Locale.ROOT)
              + " action is supported, got "
              + actions);
    }
    return (T) actions.get(0);
  }

  static List<Attribute> attributes(scala.collection.Seq<? extends Attribute> seq) {
    return new ArrayList<>(JavaConverters.seqAsJavaListConverter(seq).asJava());
  }

  static <T> List<T> seq(scala.collection.Seq<T> seq) {
    return JavaConverters.seqAsJavaListConverter(seq).asJava();
  }

  static Option<Expression> none() {
    return Option.empty();
  }
}
