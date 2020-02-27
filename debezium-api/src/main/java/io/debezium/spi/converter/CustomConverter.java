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
     * An Actual conversion converting data from one type to another.
     *
     */
    @FunctionalInterface
    interface Converter {
        Object convert(Object input);
    }

    public interface ConverterRegistration<S> {
        void register(S fieldSchema, Converter converter);
    }

    void configure(Properties props);

    /**
     * A custom converter injected by the Debezium user.
     *
     * @param field - converted field metadata
     * @return empty if the converter is not applicable for this field
     */
    void converterFor(F field, ConverterRegistration<S> registration);
}
