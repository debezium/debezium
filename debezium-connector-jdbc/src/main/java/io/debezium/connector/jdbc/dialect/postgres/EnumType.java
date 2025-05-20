/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.type.connect.ConnectStringType;
import io.debezium.data.Enum;

/**
 * An implementation of {@link Type} for {@link Enum} column types.
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
        LOGGER.warn("Cannot create enum types automatically, please create the table by hand. Using STRING fallback.");
        return ConnectStringType.INSTANCE.getTypeName(schema, isKey);
    }

}
