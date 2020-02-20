/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.converter;

import java.util.Optional;
import java.util.Properties;

import io.debezium.annotation.Incubating;

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

    /**
     * Class binding together the schema of the conversion result and the converter code.
     *
     * @param <S> schema describing the output type, usually {@link org.apache.kafka.connect.data.SchemaBuilder}
     */
    public class ConverterDefinition<S> {
        public final S fieldSchema;
        public final Converter converter;

        public ConverterDefinition(S fieldSchema, Converter converter) {
            super();
            this.fieldSchema = fieldSchema;
            this.converter = converter;
        }
    }

    void configure(Properties props);

    /**
     * A custom converter injected by the Debezium user.
     *
     * @param field - converted field metadata
     * @return empty if the converter is not applicable for this field
     */
    Optional<ConverterDefinition<S>> converterFor(F field);
}
