/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

/**
 * @author Chris Cranford
 */
final class AttributeEditorImpl implements AttributeEditor {

    private String name;
    private String value;

    protected AttributeEditorImpl() {
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
    public AttributeEditor name(String name) {
        this.name = name;
        return this;
    }

    @Override
    public AttributeEditor value(String value) {
        this.value = value;
        return this;
    }

    @Override
    public Attribute create() {
        return new AttributeImpl(name, value);
    }

    @Override
    public String toString() {
        return create().toString();
    }
}
