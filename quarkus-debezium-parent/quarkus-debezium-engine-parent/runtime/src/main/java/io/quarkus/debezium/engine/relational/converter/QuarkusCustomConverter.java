/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.relational.converter;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.relational.CustomConverterRegistry.ConverterDefinition;
import io.debezium.spi.converter.ConvertedField;

public interface QuarkusCustomConverter {
    default boolean filter(ConvertedField convertedField) {
        return true;
    }

    ConverterDefinition<SchemaBuilder> bind(ConvertedField field);
}
