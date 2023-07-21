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
 
package io.onetable.model.storage;

import java.io.IOException;
import java.io.StringWriter;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;

public class SerDe {
  private static final ObjectMapper MAPPER =
      new ObjectMapper().findAndRegisterModules().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

  public static class OneFieldKeySerializer extends JsonSerializer<OneField> {

    @Override
    public void serialize(
        OneField oneField, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      StringWriter writer = new StringWriter();
      MAPPER.writeValue(writer, oneField);
      jsonGenerator.writeFieldName(writer.toString());
    }
  }

  public static class OneFieldKeyDeserializer extends KeyDeserializer {

    @Override
    public OneField deserializeKey(String s, DeserializationContext deserializationContext)
        throws IOException {
      return MAPPER.readValue(s, OneField.class);
    }
  }

  public static class OnePartitionFieldKeySerializer extends JsonSerializer<OnePartitionField> {

    @Override
    public void serialize(
        OnePartitionField onePartitionField,
        JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider)
        throws IOException {
      StringWriter writer = new StringWriter();
      MAPPER.writeValue(writer, onePartitionField);
      jsonGenerator.writeFieldName(writer.toString());
    }
  }

  public static class OnePartitionFieldKeyDeserializer extends KeyDeserializer {

    @Override
    public OnePartitionField deserializeKey(String s, DeserializationContext deserializationContext)
        throws IOException {
      return MAPPER.readValue(s, OnePartitionField.class);
    }
  }
}
