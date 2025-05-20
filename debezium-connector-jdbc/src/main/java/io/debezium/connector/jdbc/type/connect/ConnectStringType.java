/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import java.sql.Types;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.engine.jdbc.Size;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.util.Strings;

/**
 * An implementation of {@link Type} that supports {@code STRING} connect schema types.
 *
 * @author Chris Cranford
 */
public class ConnectStringType extends AbstractConnectSchemaType {

    public static final ConnectStringType INSTANCE = new ConnectStringType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "STRING" };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        // Some source data types emit a column size, and we need to be careful about using the provided
        // size as it may be relative to the source data type; however due to the handling code on the
        // source, it may be emitting the value as a STRING.
        //
        // An example of this is BYTES data types. If the binary.handling.mode is set to anything but to
        // emit the raw bytes; the received value's schema type is STRING; however in the case of a field
        // like VARBINARY(15), the size of 15 will be included in the propagated source details and in
        // this case we should ignore it.
        //
        // To do this, getColumnSqlType will only return a non OTHER type code if the column type details
        // are provided and can be mapped to a logical CHAR/NCHAR/VARCHAR/NVARCHAR type; otherwise it
        // should be assumed that the size should be resolved using defaults rather than what was passed
        // in the propagated properties.
        final int resolvedJdbcType = getColumnSqlType(schema);
        DatabaseDialect dialect = getDialect();
        if (Types.OTHER != resolvedJdbcType) {
            // Resolved the type to CHAR/NCHAR/VARCHAR/NVARCHAR equivalent.
            // It's safe to use the specified size in the data type.
            int columnSize = getColumnSize(dialect, schema, resolvedJdbcType, isKey);

            // MySQL will not emit a column size when propagation is enabled and CHARACTER columns
            // are detected. This causes Hibernate to incorrectly report the field as "char($l)"
            // because the size won't be substituted with 1 in this case, so this will explicitly
            // set the size to 1 if the types are CHAR/NCHAR with no column size specified.
            if (columnSize == 0 && (Types.CHAR == resolvedJdbcType || Types.NCHAR == resolvedJdbcType)) {
                columnSize = 1;
            }

            if (columnSize > 0) {
                return dialect.getJdbcTypeName(resolvedJdbcType, Size.length(columnSize));
            }
            return dialect.getJdbcTypeName(resolvedJdbcType);
        }
        else {
            // The column propagation details either don't exist or did not map to a logical STRING type
            // In this case, we apply the size defaults (no size for non-keys and max-key for keys).
            final int jdbcType = hasNationalizedCharacterSet(schema) ? Types.NVARCHAR : Types.VARCHAR;
            if (isKey) {
                return dialect.getJdbcTypeName(jdbcType, Size.length(getMaxSizeInKey(dialect, jdbcType)));
            }
            return dialect.getJdbcTypeName(jdbcType);
        }
    }

    private int getColumnSize(DatabaseDialect dialect, Schema schema, int jdbcType, boolean isKey) {
        int columnSize = Integer.parseInt(getSourceColumnSize(schema).orElse("0"));
        if (!isKey) {
            return columnSize;
        }
        final int maxSizeInKey = getMaxSizeInKey(dialect, jdbcType);
        if (columnSize > 0) {
            return Math.min(columnSize, maxSizeInKey);
        }
        else {
            return maxSizeInKey;
        }
    }

    private int getMaxSizeInKey(DatabaseDialect dialect, int jdbcType) {
        if (jdbcType == Types.NCHAR || jdbcType == Types.NVARCHAR) {
            return dialect.getMaxNVarcharLengthInKey();
        }
        return dialect.getMaxVarcharLengthInKey();
    }

    private int getColumnSqlType(Schema schema) {
        final Optional<String> columnType = getSourceColumnType(schema);
        if (columnType.isPresent()) {
            final String type = columnType.get();
            // PostgreSQL represents characters as BPCHAR data types
            if (isType(type, "CHAR", "CHARACTER", "BPCHAR")) {
                return hasNationalizedCharacterSet(schema) ? Types.NCHAR : Types.CHAR;
            }
            else if (isType(type, "NCHAR")) {
                return Types.NCHAR;
            }
            else if (isType(type, "VARCHAR", "VARCHAR2", "CHARACTER VARYING")) {
                return hasNationalizedCharacterSet(schema) ? Types.NVARCHAR : Types.VARCHAR;
            }
            else if (isType(type, "NVARCHAR", "NVARCHAR2")) {
                return Types.NVARCHAR;
            }
        }
        return Types.OTHER;
    }

    private static boolean isType(String columnType, String... possibilities) {
        for (String possibility : possibilities) {
            if (possibility.equalsIgnoreCase(columnType)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasNationalizedCharacterSet(Schema schema) {
        // MySQL emits a __debezium.source.column.character_set property to pass the character set name
        // downstream in the pipeline, which is useful for the sink connector to resolve whether the
        // column should be mapped to a nationalized variant (NCHAR/NVARCHAR)
        if (schema.parameters() != null) {
            final String charsetName = schema.parameters().get("__debezium.source.column.character_set");
            return !Strings.isNullOrEmpty(charsetName) && charsetName.toLowerCase().startsWith("utf8");
        }
        return false;
    }
}
