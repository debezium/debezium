/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

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
    AttributeEditor value(String value);

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
