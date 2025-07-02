/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.converters.custom;

import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.spi.converter.ConvertedField;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.util.Strings;

public class CustomConverterServiceProvider implements ServiceProvider<CustomConverterRegistry> {

    public static final String CONVERTER_TYPE_SUFFIX = ".type";
    private final ServiceLoader<CustomConverterFactory> converterFactories = ServiceLoader.load(CustomConverterFactory.class);

    public CustomConverterServiceProvider() {
    }

    @Override
    public CustomConverterRegistry createService(Configuration configuration, ServiceRegistry serviceRegistry) {
        List<String> names = Strings.listOf(configuration.getString(CommonConnectorConfig.CUSTOM_CONVERTERS), x -> x.split(","), String::trim);

        List<CustomConverter<SchemaBuilder, ConvertedField>> customConverters = names
                .stream()
                .map(name -> createConverter(configuration, name))
                .collect(Collectors.toList());

        List<CustomConverter<SchemaBuilder, ConvertedField>> additionalConverters = converterFactories
                .findFirst()
                .map(CustomConverterFactory::get)
                .stream()
                .toList();

        customConverters.addAll(additionalConverters);

        return new CustomConverterRegistry(customConverters);
    }

    @Override
    public Class<CustomConverterRegistry> getServiceClass() {
        return CustomConverterRegistry.class;
    }

    private CustomConverter<SchemaBuilder, ConvertedField> createConverter(Configuration configuration, String name) {
        CustomConverter<SchemaBuilder, ConvertedField> converter = configuration.getInstance(name + CONVERTER_TYPE_SUFFIX, CustomConverter.class);

        converter.configure(configuration.subset(name, true).asProperties());

        return converter;
    }
}
