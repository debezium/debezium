/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.reactive.quarkus.it;

import java.util.Locale;

import javax.persistence.AttributeConverter;

/**
 * An {@link AttributeConverter} that converters the input string to upper-case at persistence time
 * and returns the database value as-is when hydrated from the persistence store.
 *
 * @author Chris Cranford
 */
public class UpperCaseAttributeConverter implements AttributeConverter<String, String> {
    @Override
    public String convertToDatabaseColumn(String s) {
        return s != null ? s.toUpperCase(Locale.ROOT) : null;
    }

    @Override
    public String convertToEntityAttribute(String s) {
        return s;
    }
}
