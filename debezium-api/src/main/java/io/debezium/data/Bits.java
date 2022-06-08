/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import java.util.BitSet;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A set of bits of arbitrary length.
 *
 * @author Randall Hauch
 */
public class Bits {

    public static final String LOGICAL_NAME = "io.debezium.data.Bits";
    public static final String LENGTH_FIELD = "length";

    /**
     * Returns a {@link SchemaBuilder} for a Bits. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @param length maximum the number of bits in the set
     * @return the schema builder
     */
    public static SchemaBuilder builder(int length) {
        return SchemaBuilder.bytes()
                .name(LOGICAL_NAME)
                .parameter(LENGTH_FIELD, Integer.toString(length))
                .version(1);
    }

    /**
     * Returns a Schema for a Bits but with all other default Schema settings.
     *
     * @param length maximum the number of bits in the set
     * @return the schema
     * @see #builder(int)
     */
    public static Schema schema(int length) {
        return builder(length).build();
    }

    /**
     * Convert a value from its logical format (BitSet) to it's encoded format.
     *
     * @param schema the schema
     * @param value the logical value
     * @return the encoded value
     */
    public static byte[] fromBitSet(Schema schema, BitSet value) {
        return value.toByteArray();
    }

    public static BitSet toBitSet(Schema schema, byte[] value) {
        return BitSet.valueOf(value);
    }
}
