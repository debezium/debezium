/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.ValueBindDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;

/**
 * An implementation of {@link Type} for {@code ARRAY} column types.
 *
 * @author Bertrand Paquet
 */

public class ArrayType extends AbstractType {

    public static final ArrayType INSTANCE = new ArrayType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "ARRAY" };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return getElementTypeName(dialect, schema, key) + "[]";
    }

    private String getElementTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        Type elementType = dialect.getSchemaType(schema.valueSchema());
        return elementType.getTypeName(dialect, schema.valueSchema(), key);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return Arrays.asList(new ValueBindDescriptor(index, null));
        }
        return List.of(new ValueBindDescriptor(index, value, java.sql.Types.ARRAY, getElementTypeName(this.getDialect(), schema, false)));
    }
}
