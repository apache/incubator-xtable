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
 
package org.apache.xtable.utilities;

import java.io.IOException;
import java.util.Locale;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;

/** Deserializes boolean literals as canonical strings and rejects all other values. */
public class StrictBooleanStringDeserializer extends StdDeserializer<String> {

  public StrictBooleanStringDeserializer() {
    super(String.class);
  }

  @Override
  public String deserialize(JsonParser parser, DeserializationContext context) throws IOException {
    if (parser.currentToken() == JsonToken.VALUE_NULL) {
      return null;
    }
    String value = parser.getText();
    if (Boolean.TRUE.toString().equalsIgnoreCase(value)
        || Boolean.FALSE.toString().equalsIgnoreCase(value)) {
      return value.toLowerCase(Locale.ROOT);
    }
    throw InvalidFormatException.from(
        parser, "Expected true or false for " + parser.currentName(), value, String.class);
  }
}
