/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import io.debezium.spi.common.ReplacementFunction;

/**
 * A field name underscore replacement implementation of {@link ReplacementFunction}
 *
 * @author Harvey Yue
 */
public class FieldNameUnderscoreReplacementFunction implements ReplacementFunction {

    @Override
    public String replace(char invalid) {
        return REPLACEMENT_CHAR;
    }

    /**
     * Sanitize column names that are illegal in Avro
     * Must conform to https://avro.apache.org/docs/1.7.7/spec.html#Names
     * Legal characters are [a-zA-Z_] for the first character and [a-zA-Z0-9_] thereafter.
     */
    @Override
    public boolean isValidNonFirstCharacter(char c) {
        return isValidFirstCharacter(c) || (c >= '0' && c <= '9');
    }
}
