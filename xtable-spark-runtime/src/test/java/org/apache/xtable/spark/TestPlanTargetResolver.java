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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.junit.jupiter.api.Test;

class TestPlanTargetResolver {

  @Test
  void nullQueryExecutionYieldsEmpty() {
    assertFalse(PlanTargetResolver.resolveWrittenPath(null).isPresent());
  }

  @Test
  void nonWritePlanYieldsEmpty() {
    QueryExecution qe = mock(QueryExecution.class);
    LogicalPlan plan = mock(LogicalPlan.class);
    when(qe.analyzed()).thenReturn(plan);
    // A read/aggregate plan is not one of the recognized write commands.
    assertFalse(PlanTargetResolver.resolveWrittenPath(qe).isPresent());
  }
}
