/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Utility class with customized {@link Schema} instances for the built-in primitives, except that these all have defaults
 * set to null in addition to being optional.
 * 
 * @author Randall Hauch
 */
public interface OptionalSchema {

    Schema OPTIONAL_INT8_SCHEMA = SchemaBuilder.int8().optional().defaultValue(null).build();
    Schema OPTIONAL_INT16_SCHEMA = SchemaBuilder.int16().optional().defaultValue(null).build();
    Schema OPTIONAL_INT32_SCHEMA = SchemaBuilder.int32().optional().defaultValue(null).build();
    Schema OPTIONAL_INT64_SCHEMA = SchemaBuilder.int64().optional().defaultValue(null).build();
    Schema OPTIONAL_FLOAT32_SCHEMA = SchemaBuilder.float32().optional().defaultValue(null).build();
    Schema OPTIONAL_FLOAT64_SCHEMA = SchemaBuilder.float64().optional().defaultValue(null).build();
    Schema OPTIONAL_BOOLEAN_SCHEMA = SchemaBuilder.bool().optional().defaultValue(null).build();
    Schema OPTIONAL_STRING_SCHEMA = SchemaBuilder.string().optional().defaultValue(null).build();
    Schema OPTIONAL_BYTES_SCHEMA = SchemaBuilder.bytes().optional().defaultValue(null).build();

}
