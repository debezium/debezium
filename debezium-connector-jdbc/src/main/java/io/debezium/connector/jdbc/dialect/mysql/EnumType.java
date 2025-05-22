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

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.data.Enum;

/**
 * An implementation of {@link JdbcType} for {@code ENUM} data types.
 *
 * @author Chris Cranford
 */
class EnumType extends AbstractType {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnumType.class);

    public static final EnumType INSTANCE = new EnumType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Enum.LOGICAL_NAME };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        // Debezium passes parameter called "allowed" which contains the value-set for the ENUM.
        final Optional<String> allowedValues = getSchemaParameter(schema, "allowed");
        if (allowedValues.isPresent()) {
            final String[] values = allowedValues.get().split(",");
            return "enum(" + Arrays.stream(values).map(v -> "'" + v + "'").collect(Collectors.joining(",")) + ")";
        }
        LOGGER.warn("ENUM was detected without any allowed values.");
        return "enum()";
    }

}
