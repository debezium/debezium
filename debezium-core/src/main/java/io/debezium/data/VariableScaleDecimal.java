/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.data;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * An arbitrary precision decimal value with variable scale.
 * 
 * @author Jiri Pechanec
 *
 */
public class VariableScaleDecimal {
    public static final String LOGICAL_NAME = "io.debezium.data.VariableScaleDecimal";
    public static final String SCALE_FIELD = "scale";
    public static final String VALUE_FIELD = "value";

    /**
     * Returns a {@link SchemaBuilder} for a VariableScaleDecimal. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     * 
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.struct()
                .name(LOGICAL_NAME)
                .version(1)
                .doc("Variable scaled decimal")
                .field(SCALE_FIELD, Schema.INT32_SCHEMA)
                .field(VALUE_FIELD, Schema.BYTES_SCHEMA);
    }

    /**
     * Returns a Schema for a VariableScaleDecimal but with all other default Schema settings.
     * 
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    /**
     * Converts a value from its logical format (BigDecimal) to it's encoded format - a struct containing
     * the scale of the number and a binary representation of the number
     * 
     * @param value the logical value
     * @return the encoded value
     */
    public static Struct fromLogical(final Schema schema, final BigDecimal value) {
        Struct result = new Struct(schema);
        return result.put(SCALE_FIELD, value.scale()).put(VALUE_FIELD, value.unscaledValue().toByteArray());
    }

    /**
     * Decodes the encoded value - see {@link #fromLogical(Schema, BigDecimal)} for encoding format
     * 
     * @param value the encoded value
     * @return the decoded value
     */
    public static BigDecimal toLogical(final Struct value) {
        return new BigDecimal(new BigInteger((byte[])value.getBytes(VALUE_FIELD)), value.getInt32(SCALE_FIELD));
    }
}
