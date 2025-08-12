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

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;

import java.nio.file.Path;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.OriginalType.DATE;
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MILLIS;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;

import java.nio.file.Path;
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ParquetParitionUtil {

    public static SparkSession getSparkSession(Path tempDir) {
        SparkConf sparkConf = new SparkConf();
        String extraJavaOptions = "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED";
        sparkConf.set("spark.driver.extraJavaOptions", extraJavaOptions);
        sparkConf.setMaster("local[4]");
        SparkSession sparkSession =
                SparkSession.builder().config(sparkConf).getOrCreate();
        sparkSession.read().parquet(tempDir.toAbsolutePath().toString());
        return sparkSession;
    }
    public static MessageType convertSparkSchemaToParquet(StructType sparkSchema) {
        Types.MessageTypeBuilder messageTypeBuilder = Types.buildMessage();
        for (StructField field : sparkSchema.fields()) {
            Type parquetType = convertField(field);
            messageTypeBuilder.addField(parquetType);
        }
        return messageTypeBuilder.named("spark_schema");
    }

    public static Type convertField(StructField field) {
        String name = field.name();
        boolean isNullable = field.nullable();
        Type.Repetition repetition = isNullable ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;

        DataType sparkType = field.dataType();

        if (sparkType instanceof StringType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition).as(UTF8).named(name);
        } else if (sparkType instanceof IntegerType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition).named(name);
        } else if (sparkType instanceof LongType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition).named(name);
        } else if (sparkType instanceof BooleanType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition).named(name);
        } else if (sparkType instanceof FloatType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition).named(name);
        } else if (sparkType instanceof DoubleType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition).named(name);
        } else if (sparkType instanceof DateType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition).as(DATE).named(name);
        } else if (sparkType instanceof TimestampType) {
            // for now INT64 only
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition).as(TIMESTAMP_MILLIS).named(name);
        } else if (sparkType instanceof BinaryType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition).named(name);
        } else if (sparkType instanceof StructType) {
            //  nested structs
            Types.GroupBuilder groupBuilder = Types.buildGroup(repetition);

            for (StructField nestedField : ((StructType) sparkType).fields()) {
                groupBuilder.addField(convertField(nestedField));
            }

            return (Type) groupBuilder.named(name);
        }

        throw new UnsupportedOperationException("Unsupported Spark DataType: " + sparkType.typeName());
    }
    public static boolean containsField(InternalSchema fileSchema, String fieldName) {

        return fileSchema.getFields().stream()
                .map(InternalField::getName)
                .anyMatch(name -> name.equals(fieldName));
    }
}
