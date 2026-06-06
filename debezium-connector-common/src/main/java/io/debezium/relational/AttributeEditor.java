/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.math.BigDecimal;
import java.math.BigInteger;

import io.debezium.annotation.NotThreadSafe;

/**
 * An editor for {@link Attribute} instances.
 *
 * @author Chris Cranford
 */
@NotThreadSafe
public interface AttributeEditor {
    /**
     * Get the name of the attribute.
     *
     * @return the attribute name; may be null if not set
     */
    String name();

    /**
     * Get the value of the attribute.
     *
     * @return the attribute value; may be null if not set
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
     * Set the name of the attribute.
     *
     * @param name the attribute name
     * @return this editor so callers can chain methods together
     */
    AttributeEditor name(String name);

    /**
     * Set the value of the attribute.
     *
     * @param value the attribute value
     * @return this editor so callers can chain methods together
     */
    AttributeEditor value(Object value);

    /**
     * Obtain an immutable attribute definition representing the current state of this editor.
     * Typically, an editor is created and used to build an attribute, and then discarded. However, this editor
     * with its current state can be reused after this method, since the resulting attribute definition no
     * longer refers to any of the data used in this editor.
     *
     * @return the immutable attribute definition; never null
     */
    Attribute create();
}
