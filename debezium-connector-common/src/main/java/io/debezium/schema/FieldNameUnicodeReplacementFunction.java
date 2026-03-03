/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

/**
 * A field name unicode replacement inheritance of {@link UnicodeReplacementFunction}
 *
 * @author Harvey Yue
 */
public class FieldNameUnicodeReplacementFunction extends UnicodeReplacementFunction {

    /**
     * Use underscore as escape sequence instead of backslash, so treat underscore as an
     * invalid character is expected.
     * Legal characters are [a-zA-Z] for the first character and [a-zA-Z0-9] thereafter.
     */
    @Override
    public boolean isValidNonFirstCharacter(char c) {
        return isValidFirstCharacter(c) || (c >= '0' && c <= '9');
    }
}
