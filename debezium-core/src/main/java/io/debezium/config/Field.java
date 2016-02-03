/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import io.debezium.annotation.Immutable;

/**
 * An immutable definition of a field that make appear within a {@link Configuration} instance.
 * 
 * @author Randall Hauch
 */
@Immutable
public final class Field {
    /**
     * A functional interface that can be used to validate field values.
     */
    public static interface Validator {
        /**
         * Validate the supplied value for the field, and report any problems to the designated consumer.
         * 
         * @param config the configuration containing the field to be validated; may not be null
         * @param field the {@link Field} being validated; never null
         * @param problems the consumer to be called with each problem; never null
         * @return the number of problems that were found, or 0 if the value is valid
         */
        int validate(Configuration config, Field field, Consumer<String> problems);
    }

    /**
     * Create an immutable {@link Field} instance with the given property name.
     * 
     * @param name the name of the field; may not be null
     * @return the field; never null
     */
    public static Field create(String name) {
        return new Field(name, null, null, null);
    }

    /**
     * Create an immutable {@link Field} instance with the given property name and description.
     * 
     * @param name the name of the field; may not be null
     * @param description the description
     * @return the field; never null
     */
    public static Field create(String name, String description) {
        return new Field(name, description, null, null);
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
        return new Field(name, description, defaultValue, null);
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
        return new Field(name, description, Integer.toString(defaultValue), null);
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
        return new Field(name, description, Long.toString(defaultValue), null);
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
        return new Field(name, description, Boolean.toString(defaultValue), null);
    }

    private final String name;
    private final String desc;
    private final String defaultValue;
    private final Validator validator;

    protected Field(String name, String description, String defaultValue, Validator validator) {
        Objects.requireNonNull(name, "The field name is required");
        this.name = name;
        this.desc = description;
        this.defaultValue = defaultValue;
        this.validator = validator;
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
     * Validate the supplied value for this field, and report any problems to the designated consumer.
     * 
     * @param config the field values keyed by their name; may not be null
     * @param problems the consumer to be called with each problem; never null
     * @return {@code true} if the value is considered valid, or {@code false} if it is not valid
     */
    public boolean validate(Configuration config, Consumer<String> problems) {
        return validator == null ? true : validator.validate(config, this, problems) == 0;
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given description.
     * 
     * @param description the new description for the new field
     * @return the new field; never null
     */
    public Field withDescription(String description) {
        return Field.create(name(), description(), defaultValue);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given default value.
     * 
     * @param defaultValue the new default value for the new field
     * @return the new field; never null
     */
    public Field withDefault(String defaultValue) {
        return Field.create(name(), description(), defaultValue);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given default value.
     * 
     * @param defaultValue the new default value for the new field
     * @return the new field; never null
     */
    public Field withDefault(boolean defaultValue) {
        return Field.create(name(), description(), defaultValue);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given default value.
     * 
     * @param defaultValue the new default value for the new field
     * @return the new field; never null
     */
    public Field withDefault(int defaultValue) {
        return Field.create(name(), description(), defaultValue);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given default value.
     * 
     * @param defaultValue the new default value for the new field
     * @return the new field; never null
     */
    public Field withDefault(long defaultValue) {
        return Field.create(name(), description(), defaultValue);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but that uses no validation.
     * 
     * @return the new field; never null
     */
    public Field withNoValidation() {
        return new Field(name(), description(), defaultValue, null);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but that uses the supplied validation function during
     * {@link Field#validate(Configuration, Consumer)}.
     * 
     * @param validator the validation function; may be null
     * @return the new field; never null
     */
    public Field withValidation(Validator validator) {
        return new Field(name(), description(), defaultValue, validator);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but that uses the supplied conversion check function
     * during {@link Field#validate(Configuration, Consumer)}.
     * 
     * @param conversionCheck the functions that attempt to validate the object; may be null
     * @return the new field; never null
     */
    @SuppressWarnings("unchecked")
    public Field withValidation(Function<String, ?>... conversionCheck) {
        return new Field(name(), description(), defaultValue, (config, field, problems) -> {
            String value = config.getString(field);
            for (Function<String, ?> check : conversionCheck) {
                if (check != null) {
                    try {
                        check.apply(value);
                    } catch (Throwable t) {
                        problems.accept("The " + field.name() + " value '" + value + "' is not allowed: " + t.getMessage());
                        return 1;
                    }
                }
            }
            return 0;
        });
    }

    /**
     * Create and return a new Field instance that that is a copy of this field but that uses the supplied predicate during
     * {@link Field#validate(Configuration, Consumer)}.
     * 
     * @param predicates the functions that attempt to validate the object; may be null
     * @return the new field; never null
     */
    @SuppressWarnings("unchecked")
    public Field withValidation(Predicate<String>... predicates) {
        return new Field(name(), description(), defaultValue, (config, field, problems) -> {
            String value = config.getString(field);
            for (Predicate<String> predicate : predicates) {
                if (predicate != null) {
                    try {
                        if (!predicate.test(value)) {
                            problems.accept("The " + field.name() + " value '" + value + "' is not valid");
                        }
                    } catch (Throwable t) {
                        problems.accept("The " + field.name() + " value '" + value + "' is not allowed: " + t.getMessage());
                        return 1;
                    }
                }
            }
            return 0;
        });
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj instanceof Field) {
            Field that = (Field) obj;
            return this.name().equals(that.name());
        }
        return false;
    }

    @Override
    public String toString() {
        return name();
    }

    public static boolean isRequired(String value) {
        return value != null && value.trim().length() > 0;
    }

    public static boolean isBoolean(String value) {
        Boolean.parseBoolean(value);
        return true;
    }

    public static boolean isInteger(String value) {
        if (value != null) Integer.parseInt(value);
        return true;
    }

    public static boolean isNonNegativeInteger(String value) {
        return value != null ? Integer.parseInt(value) >= 0 : true;
    }

    public static boolean isPositiveInteger(String value) {
        return value != null ? Integer.parseInt(value) > 0 : true;
    }
}