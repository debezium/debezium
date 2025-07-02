/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.converters.custom;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.spi.converter.ConvertedField;
import io.debezium.spi.converter.CustomConverter;

public interface CustomConverterFactory {

    CustomConverter<SchemaBuilder, ConvertedField> get();
}
