/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.relational.converter;

import java.util.List;
import java.util.Properties;

import jakarta.enterprise.inject.spi.CDI;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.converters.custom.CustomConverterFactory;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.spi.converter.ConvertedField;
import io.debezium.spi.converter.CustomConverter;

public class QuarkusCustomConverterFactory implements CustomConverterFactory {

    private final List<QuarkusCustomConverter> converters;

    public QuarkusCustomConverterFactory() {
        this.converters = CDI.current().select(QuarkusCustomConverter.class).stream().toList();
    }

    public QuarkusCustomConverterFactory(List<QuarkusCustomConverter> converters) {
        this.converters = converters;
    }

    @Override
    public CustomConverter<SchemaBuilder, ConvertedField> get() {
        return new CustomConverter<>() {
            @Override
            public void configure(Properties props) {
                // ignore
            }

            @Override
            public void converterFor(ConvertedField field, ConverterRegistration<SchemaBuilder> registration) {
                converters.forEach(converter -> {
                    if (!converter.filter().test(field)) {
                        return;
                    }
                    CustomConverterRegistry.ConverterDefinition<SchemaBuilder> definition = converter.bind(field);

                    registration.register(definition.fieldSchema, definition.converter);
                });
            }
        };
    }
}
