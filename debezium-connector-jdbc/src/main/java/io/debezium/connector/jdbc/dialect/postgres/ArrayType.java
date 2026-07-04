/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code ARRAY} column types.
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
    public String getTypeName(Schema schema, boolean isKey) {
        return getElementTypeName(getDialect(), schema, isKey) + "[]";
    }

    private String getElementTypeName(DatabaseDialect dialect, Schema schema, boolean isKey) {
        JdbcType elementJdbcType = dialect.getSchemaType(schema.valueSchema());
        return elementJdbcType.getTypeName(schema.valueSchema(), isKey);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        final String elementTypeName = baseElementTypeName(getElementTypeName(this.getDialect(), schema, false));
        return List.of(new ValueBindDescriptor(index, value, java.sql.Types.ARRAY, elementTypeName));
    }

    /**
     * Reduces a column type name to the base element type name that
     * {@link java.sql.Connection#createArrayOf(String, Object[])} accepts. The JDBC driver looks the
     * name up as a server type, so it rejects both the array brackets and any length/precision
     * modifier: {@code numeric(10,2)} must be passed as {@code numeric}, {@code varchar(255)} as
     * {@code varchar}. The DDL type name keeps the modifier; only the {@code createArrayOf} argument
     * needs the bare base type.
     */
    static String baseElementTypeName(String typeName) {
        // Strip any array brackets: numeric(10,2)[] -> numeric(10,2)
        typeName = typeName.replaceAll("\\[]", "").trim();
        // Strip the length/precision modifier: numeric(10,2) -> numeric
        final int parenIndex = typeName.indexOf('(');
        if (parenIndex > 0) {
            typeName = typeName.substring(0, parenIndex).trim();
        }
        return typeName.toLowerCase();
    }
}
