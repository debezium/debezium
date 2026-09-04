/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.util.List;
import java.util.Locale;
import java.util.Set;

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

    /**
     * PostgreSQL native array types whose element the source emits with a generic {@code STRING} or
     * {@link io.debezium.data.Json} element schema, so the real element type cannot be recovered from
     * the element schema alone. For these the element type is taken from the array field's propagated
     * source column type instead. Numeric arrays are deliberately excluded: their element schema
     * carries the precision/scale (e.g. {@code numeric(10,2)}) that must be preserved. {@code json} is
     * excluded too: unlike {@code jsonb} it already resolves correctly from the element schema.
     */
    private static final Set<String> NATIVE_ELEMENT_TYPES = Set.of(
            "inet", "cidr", "macaddr", "macaddr8",
            "int4range", "int8range", "numrange", "tsrange", "tstzrange", "daterange",
            "jsonb");

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "ARRAY" };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return getElementTypeName(getDialect(), schema, isKey) + "[]";
    }

    private String getElementTypeName(DatabaseDialect dialect, Schema schema, boolean isKey) {
        final String nativeElementType = nativeElementTypeName(getSourceColumnType(schema).orElse(null));
        if (nativeElementType != null) {
            return nativeElementType;
        }
        JdbcType elementJdbcType = dialect.getSchemaType(schema.valueSchema());
        return elementJdbcType.getTypeName(schema.valueSchema(), isKey);
    }

    /**
     * Maps an array source column type to its element type name, e.g. {@code _INET} to {@code inet}
     * (PostgreSQL names an array type after its element with a {@code _} prefix). Returns {@code null}
     * for anything not in {@link #NATIVE_ELEMENT_TYPES} so other arrays keep resolving via the element schema.
     */
    static String nativeElementTypeName(String arraySourceColumnType) {
        if (arraySourceColumnType == null || !arraySourceColumnType.startsWith("_")) {
            return null;
        }
        final String elementType = arraySourceColumnType.substring(1).toLowerCase(Locale.ROOT);
        return NATIVE_ELEMENT_TYPES.contains(elementType) ? elementType : null;
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
