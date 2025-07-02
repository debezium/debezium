/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.relational.converter;

import java.util.function.Predicate;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.relational.CustomConverterRegistry.ConverterDefinition;
import io.debezium.spi.converter.ConvertedField;

public interface QuarkusCustomConverter {
    default Predicate<ConvertedField> filter() {
        return convertedField -> true;
    }

    ConverterDefinition<SchemaBuilder> bind(ConvertedField field);
}
