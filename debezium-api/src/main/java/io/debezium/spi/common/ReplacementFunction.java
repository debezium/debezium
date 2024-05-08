/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.common;

import io.debezium.common.annotation.Incubating;

/**
 * Function used to determine the replacement for a character that is not valid per Avro rules.
 */
@Incubating
public interface ReplacementFunction {

    String REPLACEMENT_CHAR = "_";

    /**
     * Determine the replacement string for the invalid character.
     *
     * @param invalid the invalid character
     * @return the replacement string; may not be null
     */
    String replace(char invalid);

    /**
     * Determine if the supplied character is a valid first character for Avro fullnames.
     * Default legal characters are [a-zA-Z_] for the first character.
     *
     * @param c the character
     * @return {@code true} if the character is a valid first character of an Avro fullname, or {@code false} otherwise
     */
    default boolean isValidFirstCharacter(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
    }

    /**
     * Determine if the supplied character is a valid non-first character for Avro fullnames.
     * Default legal characters are [a-zA-Z0-9_.] for the non-first character.
     *
     * @param c the character
     * @return {@code true} if the character is a valid non-first character of an Avro fullname, or {@code false} otherwise
     */
    default boolean isValidNonFirstCharacter(char c) {
        return isValidFirstCharacter(c) || c == '.' || (c >= '0' && c <= '9');
    }

    ReplacementFunction UNDERSCORE_REPLACEMENT = c -> REPLACEMENT_CHAR;
}
