/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.data.EnumSet;

/**
 * An implementation of {@link Type} for {@code SET} data types.
 *
 * @author Chris Cranford
 */
class SetType extends AbstractType {

    private static final Logger LOGGER = LoggerFactory.getLogger(SetType.class);

    public static final SetType INSTANCE = new SetType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ EnumSet.LOGICAL_NAME };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        // Debezium passes parameter called "allowed" which contains the value-set for the SET.
        final Optional<String> allowedValues = getSchemaParameter(schema, "allowed");
        if (allowedValues.isPresent()) {
            final String[] values = allowedValues.get().split(",");
            return "set(" + Arrays.stream(values).map(v -> "'" + v + "'").collect(Collectors.joining(",")) + ")";
        }
        LOGGER.warn("SET was detected without any allowed values.");
        return "set()";
    }

}
