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
import org.apache.kafka.connect.errors.DataException;

/**
 * An arbitrary precision decimal value with fixed scale.
 * 
 * @author Jiri Pechanec
 *
 */
public class FixedScaleDecimal {
    public static final String LOGICAL_NAME = "io.debezium.data.FixedScaleDecimal";
    public static final String SCALE_PARAMETER = "scale";

    /**
     * Returns a {@link SchemaBuilder} for a FixedScaleDecimal. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @param scale of the number
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder(int scale) {
        return DebeziumDecimal.builder()
                .name(LOGICAL_NAME)
                .version(1)
                .doc("Fixed scaled decimal")
                .parameter(SCALE_PARAMETER, Integer.toString(scale));
    }

    /**
     * Returns a Schema for a FixedScaleDecimal but with all other default Schema settings.
     *
     * @param scale of the number
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema(int scale) {
        return builder(scale).build();
    }

    /**
     * Returns a Schema for an optional FixedScaleDecimal but with all other default Schema settings.
     *
     * @param scale of the number
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema optionalSchema(int scale) {
        return builder(scale).optional().build();
    }

    /**
     * Converts a value from its logical format (BigDecimal) to it's encoded format - a struct containing
     * the scale of the number and a binary representation of the number
     *
     * @param value the logical value
     * @return the encoded value
     */
    public static Struct fromLogical(Schema schema, DebeziumDecimal value) {
        Struct result = new Struct(schema);
        value.forDecimalValue(x -> {
            if (x.scale() != scale(schema)) {
                throw new DataException("BigDecimal has mismatching scale value for given Decimal schema");
            }
        });
        value.fromLogical(result);
        return result;
    }

    /**
     * Decodes the encoded value - see {@link #fromLogical(Schema, Struct)} for encoding format
     * 
     * @param value the encoded value
     * @return the decoded value
     */
    public static DebeziumDecimal toLogical(Schema schema, final Struct value) {
        return DebeziumDecimal.toLogical(value, (x) -> new BigDecimal(x, scale(schema)));
    }

    private static int scale(Schema schema) {
        String scaleString = schema.parameters().get(SCALE_PARAMETER);
        if (scaleString == null)
            throw new DataException("Invalid " + LOGICAL_NAME + " schema: scale parameter not found.");
        try {
            return Integer.parseInt(scaleString);
        }
        catch (NumberFormatException e) {
            throw new DataException("Invalid scale parameter found in " + LOGICAL_NAME + " schema: ", e);
        }
    }
}
