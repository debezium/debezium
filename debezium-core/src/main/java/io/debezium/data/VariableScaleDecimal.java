/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.data;

import java.math.BigDecimal;

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
    public static final Struct ZERO = fromLogical(schema(), DebeziumDecimal.ZERO);
    /**
     * Returns a {@link SchemaBuilder} for a VariableScaleDecimal. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return DebeziumDecimal.builder()
                .name(LOGICAL_NAME)
                .version(1)
                .doc("Variable scaled decimal")
                .field(SCALE_FIELD, Schema.OPTIONAL_INT32_SCHEMA);
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
     * Converts a value from its logical format (BigDecimal or SpecialValue) to its encoded format - a struct containing
     * the scale of the number and a binary representation of the number or special value.
     *
     * @param value the value or the  decimal
     * @param special value of the number
     *
     * @return the encoded value
     */
    public static Struct fromLogical(Schema schema, DebeziumDecimal value) {
        Struct result = new Struct(schema);
        value.fromLogical(result);
        value.forDecimalValue(x -> result.put(SCALE_FIELD, x.scale()));
        return result;
    }

    /**
     * Decodes the encoded value - see {@link #fromLogical(Schema, BigDecimal)} for encoding format
     *
     * @param value the encoded value
     * @return the decoded value
     */
    public static DebeziumDecimal toLogical(final Struct value) {
        return DebeziumDecimal.toLogical(value, (x) -> new BigDecimal(x, value.getInt32(SCALE_FIELD)));
    }
}
