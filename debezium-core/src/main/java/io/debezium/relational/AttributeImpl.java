/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Objects;

/**
 * Relational model implementation of {@link Attribute}.
 *
 * @author Chris Cranford
 */
final class AttributeImpl implements Attribute {

    private final String name;
    private final String value;

    public AttributeImpl(String name, String value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String value() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof Attribute) {
            AttributeImpl attribute = (AttributeImpl) obj;
            return Objects.equals(name, attribute.name) && Objects.equals(value, attribute.value);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    @Override
    public String toString() {
        return "name='" + name + "', value='" + value + "'";
    }

    @Override
    public AttributeEditor edit() {
        return Attribute.editor().name(name()).value(value());
    }
}
