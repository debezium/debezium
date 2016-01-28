/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.util.Objects;

import io.debezium.annotation.Immutable;

/**
 * An immutable definition of a field that make appear within a {@link Configuration} instance.
 * 
 * @author Randall Hauch
 */
@Immutable
public final class Field {
    /**
     * Create an immutable {@link Field} instance with the given property name and description.
     * 
     * @param name the name of the field; may not be null
     * @param description the description
     * @return the field; never null
     */
    public static Field create(String name, String description) {
        return new Field(name, description, null);
    }

    /**
     * Create an immutable {@link Field} instance with the given property name, description, and default value.
     * 
     * @param name the name of the field; may not be null
     * @param description the description
     * @param defaultValue the default value for the field
     * @return the field; never null
     */
    public static Field create(String name, String description, String defaultValue) {
        return new Field(name, description, defaultValue);
    }

    /**
     * Create an immutable {@link Field} instance with the given property name, description, and default value.
     * 
     * @param name the name of the field; may not be null
     * @param description the description
     * @param defaultValue the default value for the field
     * @return the field; never null
     */
    public static Field create(String name, String description, int defaultValue) {
        return new Field(name, description, Integer.toString(defaultValue));
    }

    /**
     * Create an immutable {@link Field} instance with the given property name, description, and default value.
     * 
     * @param name the name of the field; may not be null
     * @param description the description
     * @param defaultValue the default value for the field
     * @return the field; never null
     */
    public static Field create(String name, String description, long defaultValue) {
        return new Field(name, description, Long.toString(defaultValue));
    }

    /**
     * Create an immutable {@link Field} instance with the given property name, description, and default value.
     * 
     * @param name the name of the field; may not be null
     * @param description the description
     * @param defaultValue the default value for the field
     * @return the field; never null
     */
    public static Field create(String name, String description, boolean defaultValue) {
        return new Field(name, description, Boolean.toString(defaultValue));
    }

    private final String name;
    private final String desc;
    private final String defaultValue;

    protected Field(String name, String description, String defaultValue) {
        Objects.requireNonNull(name, "The field name is required");
        this.name = name;
        this.desc = description;
        this.defaultValue = defaultValue;
        assert this.name != null;
    }

    /**
     * Get the name of the field.
     * 
     * @return the name; never null
     */
    public String name() {
        return name;
    }

    /**
     * Get the default value of the field.
     * 
     * @return the default value as a string; never null
     */
    public String defaultValue() {
        return defaultValue;
    }

    /**
     * Get the description of the field.
     * 
     * @return the description; never null
     */
    public String description() {
        return desc;
    }

    /**
     * Create and return a new Field instance that has the same name and description as this instance, but with the given
     * default value.
     * 
     * @param defaultValue the new default value for the new field
     * @return the new field; never null
     */
    public Field withDefault(String defaultValue) {
        return Field.create(name(), description(), defaultValue);
    }

    /**
     * Create and return a new Field instance that has the same name and description as this instance, but with the given
     * default value.
     * 
     * @param defaultValue the new default value for the new field
     * @return the new field; never null
     */
    public Field withDefault(boolean defaultValue) {
        return Field.create(name(), description(), defaultValue);
    }

    /**
     * Create and return a new Field instance that has the same name and description as this instance, but with the given
     * default value.
     * 
     * @param defaultValue the new default value for the new field
     * @return the new field; never null
     */
    public Field withDefault(int defaultValue) {
        return Field.create(name(), description(), defaultValue);
    }

    /**
     * Create and return a new Field instance that has the same name and description as this instance, but with the given
     * default value.
     * 
     * @param defaultValue the new default value for the new field
     * @return the new field; never null
     */
    public Field withDefault(long defaultValue) {
        return Field.create(name(), description(), defaultValue);
    }
    
    @Override
    public int hashCode() {
        return name.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if ( obj == this ) return true;
        if ( obj instanceof Field ) {
            Field that = (Field)obj;
            return this.name().equals(that.name());
        }
        return false;
    }
    
    @Override
    public String toString() {
        return name();
    }
}