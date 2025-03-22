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
 
package org.apache.xtable.parquet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import org.apache.parquet.schema.*;
import org.junit.jupiter.api.Assertions;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;

// Test class added ONLY to cover main() invocation not covered by application tests.
public class TestParquetSchemaExtractor {
  private static final ParquetSchemaExtractor schemaExtractor =
      ParquetSchemaExtractor.getInstance();

  @Test
  public void testPrimitiveTypes() {
    //    InternalSchema primitive1 =
    // InternalSchema.builder().name("integer").dataType(InternalType.INT);
    /*   InternalSchema primitive2 =
    InternalSchema.builder().name("string").dataType(InternalType.STRING);*/
    MessageType integerPrimitiveType = null;

    Assertions.assertEquals(
        null, schemaExtractor.toInternalSchema(integerPrimitiveType, null, null));
  }

  @Test
  public void main() {
    testPrimitiveTypes();
  }
}
