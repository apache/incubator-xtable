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
 
package org.apache.xtable.hudi;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;

import org.junit.jupiter.api.Test;

public class TestHudiInstantUtils {

  @Test
  public void testParseCommitTimeToInstant() {
    assertEquals(
        Instant.parse("2023-01-20T04:43:31.843Z"),
        HudiInstantUtils.parseFromInstantTime("20230120044331843"));
    assertEquals(
        Instant.parse("2023-01-20T04:43:31.999Z"),
        HudiInstantUtils.parseFromInstantTime("20230120044331"));
  }

  @Test
  public void testInstantToCommit() {
    assertEquals(
        "20230120044331843",
        HudiInstantUtils.convertInstantToCommit(Instant.parse("2023-01-20T04:43:31.843Z")));
    assertEquals(
        "20230120044331000",
        HudiInstantUtils.convertInstantToCommit(Instant.parse("2023-01-20T04:43:31Z")));
  }
}
