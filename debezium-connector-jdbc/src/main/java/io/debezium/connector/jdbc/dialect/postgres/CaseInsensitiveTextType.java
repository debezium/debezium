/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.sink.column.ColumnDescriptor;

/**
 * An implementation of {@link Type} for {@code CITEXT} column types.
 *
 * @author Chris Cranford
 */
class CaseInsensitiveTextType extends AbstractType {

    public static final CaseInsensitiveTextType INSTANCE = new CaseInsensitiveTextType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "CITEXT" };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as citext)";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "citext";
    }

}
