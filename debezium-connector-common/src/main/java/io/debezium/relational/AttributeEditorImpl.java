/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.math.BigDecimal;
import java.math.BigInteger;

import io.debezium.DebeziumException;

/**
 * Implementation of the {@link AttributeEditor} contract.
 *
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
    public String asString() {
        return value;
    }

    @Override
    public Integer asInteger() {
        return value == null ? null : Integer.parseInt(value);
    }

    @Override
    public Long asLong() {
        return value == null ? null : Long.parseLong(value);
    }

    @Override
    public Boolean asBoolean() {
        return value == null ? null : Boolean.parseBoolean(value);
    }

    @Override
    public BigInteger asBigInteger() {
        return value == null ? null : new BigInteger(value);
    }

    @Override
    public BigDecimal asBigDecimal() {
        return value == null ? null : new BigDecimal(value);
    }

    @Override
    public Float asFloat() {
        return value == null ? null : Float.parseFloat(value);
    }

    @Override
    public Double asDouble() {
        return value == null ? null : Double.parseDouble(value);
    }

    @Override
    public AttributeEditor name(String name) {
        this.name = name;
        return this;
    }

    @Override
    public AttributeEditor value(Object value) {
        if (value == null) {
            this.value = null;
        }
        else if (value instanceof String) {
            this.value = (String) value;
        }
        else if (value instanceof Integer) {
            this.value = Integer.toString((Integer) value);
        }
        else if (value instanceof Long) {
            this.value = Long.toString((Long) value);
        }
        else if (value instanceof Boolean) {
            this.value = Boolean.toString((Boolean) value);
        }
        else if (value instanceof BigInteger) {
            this.value = value.toString();
        }
        else if (value instanceof BigDecimal) {
            this.value = value.toString();
        }
        else if (value instanceof Float) {
            this.value = Float.toString((Float) value);
        }
        else if (value instanceof Double) {
            this.value = Double.toString((Double) value);
        }
        else {
            throw new DebeziumException(value.getClass().getName() + " cannot be used for attribute values");
        }
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
