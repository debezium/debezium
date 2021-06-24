/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.converter;

import java.util.Properties;

import io.debezium.common.annotation.Incubating;

/**
 * An interface that allows the user to customize how a value will be converted for a given field.
 *
 * @author Jiri Pechanec
 */
@Incubating
public interface CustomConverter<S, F extends ConvertedField> {

    /**
     * A conversion converting data from one type to another.
     */
    @FunctionalInterface
    interface Converter {
        Object convert(Object input);
    }

    /**
     * Callback for registering a converter.
     */
    public interface ConverterRegistration<S> {

        /**
         * Registers the given schema and converter for the current field. Should not be
         * invoked more than once for the same field.
         */
        void register(S fieldSchema, Converter converter);
    }

    void configure(Properties props);

    /**
     * Allows to register a custom value and schema converter for a given field-
     *
     * @param field the field of interest
     * @param registration a callback that allows to register a schema and value converter for the given field.
     */
    void converterFor(F field, ConverterRegistration<S> registration);
}
