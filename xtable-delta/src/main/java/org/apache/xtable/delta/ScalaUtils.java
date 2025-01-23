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
 
package org.apache.xtable.delta;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.JavaConverters;

public class ScalaUtils {
  public static scala.collection.immutable.Map convertJavaMapToScala(Map<String, String> javaMap) {
    List<Tuple2<String, String>> tuples =
        javaMap.entrySet().stream()
            .map(e -> Tuple2.apply(e.getKey(), e.getValue()))
            .collect(Collectors.toList());
    scala.collection.Seq<Tuple2<String, String>> seq = JavaConverters.asScalaBuffer(tuples).toSeq();
    return scala.collection.immutable.Map$.MODULE$.apply(seq);
  }
}
