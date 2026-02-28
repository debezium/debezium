/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import io.debezium.common.annotation.Incubating;
import io.debezium.spi.common.ReplacementFunction;

/**
 * An unicode replacement implementation of {@link ReplacementFunction}
 *
 * @author Harvey Yue
 */
@Incubating
public class UnicodeReplacementFunction implements ReplacementFunction {

    @Override
    public String replace(char invalid) {
        String hex = Integer.toHexString(invalid);
        if (hex.length() <= 2) {
            hex = "00" + hex;
        }
        // Use underscore as escape sequence instead of backslash
        return "_u" + hex;
    }

    /**
     * Use underscore as escape sequence instead of backslash in UnicodeReplacementFunction, so treat underscore as an
     * invalid character is expected.
     * Legal characters are [a-zA-Z] for the first character and [a-zA-Z0-9.] thereafter.
     */
    @Override
    public boolean isValidFirstCharacter(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }
}
