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
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.type.connect.ConnectStringType;
import io.debezium.data.Enum;
import io.debezium.sink.column.ColumnDescriptor;

/**
 * An implementation of {@link JdbcType} for {@link Enum} column types.
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
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        // PostgreSQL does not implicitly cast a bound character varying value to an enum column, so the
        // cast must be explicit.
        return "cast(? as " + quoteTypeName(column.getTypeName()) + ")";
    }

    /**
     * Quotes the column's type name for use in a cast. The driver reports the name qualified and quoted
     * only for a type that is not on the search path; otherwise it reports {@code pg_type.typname}
     * verbatim, which PostgreSQL would fold to lower case unless it is quoted here.
     */
    private static String quoteTypeName(String typeName) {
        if (typeName.startsWith("\"")) {
            return typeName;
        }
        return "\"" + typeName.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        LOGGER.warn("Cannot create enum types automatically, please create the table by hand. Using STRING fallback.");
        return ConnectStringType.INSTANCE.getTypeName(schema, isKey);
    }

}
