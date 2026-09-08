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

import org.apache.spark.sql.SparkSessionExtensions;

import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

/** Registered under {@code spark.sql.extensions}; the driver plugin appends it automatically. */
public class HudiIcebergExtensions extends AbstractFunction1<SparkSessionExtensions, BoxedUnit> {
  @Override
  public BoxedUnit apply(SparkSessionExtensions extensions) {
    extensions.injectPostHocResolutionRule(
        new AbstractFunction1<
            org.apache.spark.sql.SparkSession,
            org.apache.spark.sql.catalyst.rules.Rule<
                org.apache.spark.sql.catalyst.plans.logical.LogicalPlan>>() {
          @Override
          public org.apache.spark.sql.catalyst.rules.Rule<
                  org.apache.spark.sql.catalyst.plans.logical.LogicalPlan>
              apply(org.apache.spark.sql.SparkSession session) {
            // Runs while the session's analyzer is being built, i.e. before its first catalog
            // lookup, so a catalog the builder options reverted is still rewritten in time.
            HudiCatalogRewrite.rewrite(session);
            return new HudiIcebergWriteRule(session);
          }
        });
    return BoxedUnit.UNIT;
  }
}
