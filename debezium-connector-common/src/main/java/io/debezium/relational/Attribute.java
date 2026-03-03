/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.math.BigDecimal;
import java.math.BigInteger;

import io.debezium.annotation.Immutable;

/**
 * An immutable attribute associated with a relational table.
 *
 * @author Chris Cranford
 */
@Immutable
public interface Attribute {
    /**
     * Obtain an attribute editor that can be used to define an attribute.
     *
     * @return the editor; never null
     */
    static AttributeEditor editor() {
        return new AttributeEditorImpl();
    }

    /**
     * The attribute name.
     *
     * @return the name of the attribute, never null
     */
    String name();

    /**
     * The attribute value.
     *
     * @return the value of the attribute, may be null
     */
    String value();

    /**
     * Get the attribute value as a string value.
     *
     * @return the attribute value converted to a {@link String}, may be null
     */
    String asString();

    /**
     * Get the attribute value as an integer value.
     *
     * @return the attribute value converted to an {@link Integer}, may be null
     */
    Integer asInteger();

    /**
     * Get the attribute value as a long value.
     *
     * @return the attribute value converted to a {@link Long}, may be null
     */
    Long asLong();

    /**
     * Get the attribute value as a boolean value.
     * This conversion is based on {@link Boolean#parseBoolean(String)} semantics.
     *
     * @return the attribute value converted to a {@link Boolean}, may be null
     */
    Boolean asBoolean();

    /**
     * Get the attribute value as a big integer value.
     *
     * @return the attribute value converted to a {@link BigInteger}, may be null
     */
    BigInteger asBigInteger();

    /**
     * Get the attribute value as a big decimal value.
     *
     * @return the attribute value converted to a {@link BigDecimal}, may be null
     */
    BigDecimal asBigDecimal();

    /**
     * Get the attribute value as a float value.
     *
     * @return the attribute value converted to a {@link Float}, may be null
     */
    Float asFloat();

    /**
     * Get the attribute value as a double value.
     *
     * @return the attribute value converted to a {@link Double}, may be null
     */
    Double asDouble();

    /**
     * Obtain an editor that contains the same information as this attribute.
     *
     * @return the editor; never null
     */
    AttributeEditor edit();
}
