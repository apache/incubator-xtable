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
 
package org.apache.xtable.spark.extensions.parser

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

import org.apache.xtable.spark.extensions.plans.RefreshIndex

/**
 * Parses {@code REFRESH INDEX name ON table}, which Spark's grammar does not have, and passes all
 * other statements to the parser it wraps. Spark already parses {@code CREATE INDEX} and
 * {@code DROP INDEX}.
 */
class XTableSqlParser(delegate: ParserInterface) extends ParserInterface {
  import XTableSqlParser._

  override def parsePlan(sqlText: String): LogicalPlan = sqlText match {
    case RefreshIndexStatement(indexName, tableName) =>
      RefreshIndex(
        UnresolvedTable(delegate.parseMultipartIdentifier(tableName), "REFRESH INDEX", None),
        delegate.parseMultipartIdentifier(indexName) match {
          case Seq(name) => name
          case parts =>
            throw new IllegalArgumentException(
              s"Invalid index name '${parts.mkString(".")}' in: $sqlText")
        }
      )
    case _ => delegate.parsePlan(sqlText)
  }

  override def parseExpression(sqlText: String): Expression = delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType = delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType = delegate.parseDataType(sqlText)

  override def parseQuery(sqlText: String): LogicalPlan = delegate.parseQuery(sqlText)
}

object XTableSqlParser {
  private val RefreshIndexStatement =
    """(?is)\s*REFRESH\s+INDEX\s+(`[^`]+`|\S+)\s+ON\s+(?:TABLE\s+)?(.+?)\s*;?\s*""".r
}
