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
     * Resolves the mode for a field and destination column. A textual mode applies only to an
     * unnamed {@code BYTES} schema that targets a character column. {@link BinaryHandlingMode#BYTES}
     * indicates that the regular binding applies.
     */
    public static BinaryHandlingMode resolve(JdbcSinkConnectorConfig config, String topicName, FieldDescriptor field, ColumnDescriptor column) {
        if (column == null || !isPlainBytesSchema(field.getSchema()) || !isCharacterType(column.getJdbcType())) {
            return BinaryHandlingMode.BYTES;
        }
        return config.getBinaryHandlingMode(topicName, field.getName());
    }

    /**
     * Returns whether the schema is an unnamed {@code BYTES} schema. Named logical schemas such as
     * {@code Decimal} and {@code Bits} use separate type mappings and are not subject to this setting.
     */
    public static boolean isPlainBytesSchema(Schema schema) {
        return schema.type() == Schema.Type.BYTES && schema.name() == null;
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
}
