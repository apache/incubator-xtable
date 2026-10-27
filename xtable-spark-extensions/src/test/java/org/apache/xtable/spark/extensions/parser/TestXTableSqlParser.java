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
 
package org.apache.xtable.spark.extensions.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Arrays;

import org.apache.spark.sql.catalyst.analysis.UnresolvedTable;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser$;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import scala.collection.JavaConverters;

import org.apache.xtable.spark.extensions.plans.RefreshIndex;

public class TestXTableSqlParser {
  private final XTableSqlParser parser = new XTableSqlParser(CatalystSqlParser$.MODULE$);

  @ParameterizedTest
  @ValueSource(
      strings = {
        "REFRESH INDEX email_idx ON cat.db.events",
        "refresh index email_idx on cat.db.events;",
        "  REFRESH INDEX `email_idx` ON TABLE cat.db.events  ",
        "REFRESH\nINDEX email_idx\nON cat.`db`.events"
      })
  void parsesRefreshIndex(String sql) {
    LogicalPlan plan = parser.parsePlan(sql);
    assertInstanceOf(RefreshIndex.class, plan);
    RefreshIndex refreshIndex = (RefreshIndex) plan;
    assertEquals("email_idx", refreshIndex.indexName());
    UnresolvedTable table = (UnresolvedTable) refreshIndex.table();
    assertEquals(
        Arrays.asList("cat", "db", "events"),
        JavaConverters.seqAsJavaList(table.multipartIdentifier()));
  }

  @Test
  void passesOtherStatementsToTheDelegate() {
    assertEquals("RefreshTable", parser.parsePlan("REFRESH TABLE cat.db.events").nodeName());
    assertEquals(
        "CreateIndex",
        parser.parsePlan("CREATE INDEX i ON cat.db.events USING xtable (email)").nodeName());
    assertEquals(
        Arrays.asList("cat", "db", "events"),
        JavaConverters.seqAsJavaList(parser.parseMultipartIdentifier("cat.db.events")));
  }
}
