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

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.Assignment;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeAction;
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable$;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.iceberg.spark.Spark3Util;

import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Outermost SQL parser. Iceberg's Spark 3.4 extension turns UPDATE and MERGE on any Iceberg table
 * into its own plan nodes at parse time and rewrites them in its own resolution rules, before any
 * other extension can claim them. For Hudi-managed tables this parser turns those nodes back into
 * Spark's {@code UpdateTable} / {@code MergeIntoTable}, which Iceberg's rules ignore and the Hudi
 * write rule handles. On Spark versions where Iceberg leaves the Spark nodes alone this is a no-op.
 */
public class HudiIcebergSqlParser implements ParserInterface {
  private static final Logger LOG = LoggerFactory.getLogger(HudiIcebergSqlParser.class);
  private static final String ICEBERG_UPDATE =
      "org.apache.spark.sql.catalyst.plans.logical.UpdateIcebergTable";
  private static final String ICEBERG_MERGE =
      "org.apache.spark.sql.catalyst.plans.logical.UnresolvedMergeIntoIcebergTable";

  private final SparkSession spark;
  private final ParserInterface delegate;

  public HudiIcebergSqlParser(SparkSession spark, ParserInterface delegate) {
    this.spark = spark;
    this.delegate = delegate;
  }

  @Override
  public LogicalPlan parsePlan(String sqlText) throws ParseException {
    LogicalPlan plan = delegate.parsePlan(sqlText);
    try {
      String className = plan.getClass().getName();
      if (ICEBERG_UPDATE.equals(className)) {
        LogicalPlan table = (LogicalPlan) call(plan, "table");
        if (isManaged(table)) {
          @SuppressWarnings("unchecked")
          Seq<Assignment> assignments = (Seq<Assignment>) call(plan, "assignments");
          @SuppressWarnings("unchecked")
          Option<Expression> condition = (Option<Expression>) call(plan, "condition");
          return new UpdateTable(table, assignments, condition);
        }
      } else if (ICEBERG_MERGE.equals(className)) {
        LogicalPlan target = (LogicalPlan) call(plan, "targetTable");
        if (isManaged(target)) {
          LogicalPlan source = (LogicalPlan) call(plan, "sourceTable");
          Object context = call(plan, "context");
          Expression condition = (Expression) call(context, "mergeCondition");
          @SuppressWarnings("unchecked")
          Seq<MergeAction> matched = (Seq<MergeAction>) call(context, "matchedActions");
          @SuppressWarnings("unchecked")
          Seq<MergeAction> notMatched = (Seq<MergeAction>) call(context, "notMatchedActions");
          List<MergeAction> none = Collections.emptyList();
          return MergeIntoTable$.MODULE$.apply(
              target,
              source,
              condition,
              matched,
              notMatched,
              JavaConverters.asScalaBufferConverter(none).asScala().toSeq());
        }
      }
    } catch (ReflectiveOperationException | RuntimeException e) {
      LOG.warn("Leaving {} to Iceberg: {}", plan.getClass().getSimpleName(), e.toString());
    }
    return plan;
  }

  private boolean isManaged(LogicalPlan table) {
    LogicalPlan relation = table;
    while (relation instanceof SubqueryAlias) {
      relation = ((SubqueryAlias) relation).child();
    }
    if (!(relation instanceof UnresolvedRelation)) {
      return false;
    }
    List<String> parts =
        JavaConverters.seqAsJavaListConverter(((UnresolvedRelation) relation).multipartIdentifier())
            .asJava();
    try {
      Spark3Util.CatalogAndIdentifier catalogAndIdentifier =
          Spark3Util.catalogAndIdentifier(spark, parts);
      if (!(catalogAndIdentifier.catalog() instanceof HudiSparkCatalog)) {
        return false;
      }
      Table loaded =
          ((HudiSparkCatalog) catalogAndIdentifier.catalog())
              .loadTable(catalogAndIdentifier.identifier());
      return loaded instanceof HudiSparkTable;
    } catch (Exception e) {
      return false;
    }
  }

  private static Object call(Object target, String method) throws ReflectiveOperationException {
    Method m = target.getClass().getMethod(method);
    return m.invoke(target);
  }

  @Override
  public Expression parseExpression(String sqlText) throws ParseException {
    return delegate.parseExpression(sqlText);
  }

  @Override
  public TableIdentifier parseTableIdentifier(String sqlText) throws ParseException {
    return delegate.parseTableIdentifier(sqlText);
  }

  @Override
  public FunctionIdentifier parseFunctionIdentifier(String sqlText) throws ParseException {
    return delegate.parseFunctionIdentifier(sqlText);
  }

  @Override
  public Seq<String> parseMultipartIdentifier(String sqlText) throws ParseException {
    return delegate.parseMultipartIdentifier(sqlText);
  }

  @Override
  public StructType parseTableSchema(String sqlText) throws ParseException {
    return delegate.parseTableSchema(sqlText);
  }

  @Override
  public DataType parseDataType(String sqlText) throws ParseException {
    return delegate.parseDataType(sqlText);
  }

  @Override
  public LogicalPlan parseQuery(String sqlText) throws ParseException {
    return delegate.parseQuery(sqlText);
  }
}
