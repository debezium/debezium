/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util;

import java.sql.Types;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.type.RawBytesJdbcType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.field.FieldDescriptor;

/**
 * Resolves how the JDBC sink binds {@code BYTES} fields to destination columns.
 *
 * @author Minjae Lee
 */
public final class BinaryHandling {

    private BinaryHandling() {
    }

    /**
     * Resolves how a field is bound to its destination column. A textual mode applies only when the
     * field resolves to a raw {@code BYTES} JDBC type and targets a character column.
     */
    public static Resolution resolve(JdbcSinkConnectorConfig config, String topicName, FieldDescriptor field, JdbcType jdbcType, ColumnDescriptor column) {
        if (column == null || !isRawBytesSchema(field.getSchema(), jdbcType) || !isCharacterType(column.getJdbcType())) {
            return Resolution.bytes(column);
        }
        return new Resolution(config.getBinaryHandlingMode(topicName, field.getName()), column);
    }

    /**
     * Returns whether the schema resolves to a JDBC type that binds values as raw bytes. Logical
     * types such as {@code Decimal} and {@code Bits} use separate type mappings and are excluded.
     */
    public static boolean isRawBytesSchema(Schema schema, JdbcType jdbcType) {
        return schema.type() == Schema.Type.BYTES && jdbcType instanceof RawBytesJdbcType;
    }

    /**
     * Returns whether the JDBC type is a character type.
     */
    public static boolean isCharacterType(int jdbcType) {
        switch (jdbcType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
                return true;
            default:
                return false;
        }
    }

    /**
     * The resolved binary handling mode and its destination column. The column may be {@code null}
     * when the destination could not be resolved; {@link #isEncoded()} then always returns
     * {@code false}, so callers only read {@link #targetColumn()} for encoded resolutions.
     */
    public record Resolution(BinaryHandlingMode mode, ColumnDescriptor targetColumn) {

        public static Resolution bytes(ColumnDescriptor targetColumn) {
            return new Resolution(BinaryHandlingMode.BYTES, targetColumn);
        }

        public boolean isEncoded() {
            return mode != BinaryHandlingMode.BYTES;
        }
    }
}
