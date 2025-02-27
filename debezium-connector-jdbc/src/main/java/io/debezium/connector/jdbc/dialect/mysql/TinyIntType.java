/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;

/**
 * An implementation of {@link Type} that provides support for {@code TINYINT} data types.
 *
 * @author Chris Cranford
 */
public class TinyIntType extends AbstractType {

    public static final TinyIntType INSTANCE = new TinyIntType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "TINYINT" };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        final int columnSize = Integer.parseInt(getSourceColumnSize(schema).orElse("0"));
        if (columnSize > 0) {
            return String.format("tinyint(%d)", columnSize);
        }
        return "tinyint";
    }
}
