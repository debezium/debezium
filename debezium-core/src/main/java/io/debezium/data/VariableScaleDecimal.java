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
    public static final String VALUE_FIELD = "value";
    public static final String SCALE_FIELD = "scale";
    public static final Struct ZERO = fromLogical(schema(), SpecialValueDecimal.ZERO);

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
     * Returns a Schema for an optional VariableScaleDecimal but with all other default Schema settings.
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema optionalSchema() {
        return builder().optional().build();
    }

    /**
     * Converts a value from its logical format (BigDecimal) to its encoded format - a struct containing
     * the scale of the number and a binary representation of the number.
     *
     * @param schema of the encoded value
     * @param value the value or the decimal
     *
     * @return the encoded value
     */
    public static Struct fromLogical(Schema schema, SpecialValueDecimal value) {
        Struct result = new Struct(schema);
        final BigDecimal decimalValue = value.getDecimalValue().orElse(null);
        assert decimalValue != null : "Unable to encode special value";
        result.put(VALUE_FIELD, decimalValue.unscaledValue().toByteArray());
        result.put(SCALE_FIELD, decimalValue.scale());
        return result;
    }

    /**
     * Decodes the encoded value - see {@link #fromLogical(Schema, BigDecimal)} for encoding format
     *
     * @param value the encoded value
     * @return the decoded value
     */
    public static SpecialValueDecimal toLogical(final Struct value) {
        return new SpecialValueDecimal(new BigDecimal(new BigInteger((byte[])value.getBytes(VALUE_FIELD)), value.getInt32(SCALE_FIELD)));
    }
}
