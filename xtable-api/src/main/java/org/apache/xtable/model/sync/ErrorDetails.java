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
 
package org.apache.xtable.model.sync;

import lombok.Builder;
import lombok.Value;

import org.apache.xtable.annotations.Evolving;

@Value
@Builder
@Evolving
public class ErrorDetails {
  // error Message if any
  String errorMessage;
  // Readable description of the error
  String errorDescription;
  // Can the client retry for this type of error (Transient error=true, persistent error=false)
  boolean canRetryOnFailure;

  public static ErrorDetails create(Exception e, String errorDescription) {
    if (e == null) {
      return null;
    }

    return ErrorDetails.builder()
        .errorMessage(e.getMessage())
        .errorDescription(errorDescription)
        .canRetryOnFailure(true)
        .build();
  }
}
