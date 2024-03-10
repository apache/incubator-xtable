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
 
package org.apache.xtable.model.exception;

public enum OneTableErrorCode {
  INVALID_CONFIGURATION(10001),
  INVALID_PARTITION_SPEC(10002),
  INVALID_PARTITION_VALUE(10003),
  IO_EXCEPTION(10004),
  INVALID_SCHEMA(10005),
  UNSUPPORTED_SCHEMA_TYPE(10006),
  UNSUPPORTED_FEATURE(10007),
  PARSE_EXCEPTION(10008);

  private final int errorCode;

  OneTableErrorCode(int errorCode) {
    this.errorCode = errorCode;
  }

  public int getErrorCode() {
    return errorCode;
  }
}
