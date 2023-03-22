/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type;

import java.util.Optional;

import org.apache.kafka.connect.data.Schema;

/**
 * An abstract temporal implementation of {@link Type} for {@code TIMESTAMP} based columns.
 *
 * @author Chris Cranford
 */
public abstract class AbstractTimestampType extends AbstractType {
    protected int getTimePrecision(Schema schema) {
        final String length = getSourceColumnSize(schema).orElse("0");
        final Optional<String> scale = getSourceColumnPrecision(schema);
        return scale.map(Integer::parseInt).orElseGet(() -> Integer.parseInt(length));
    }
}
