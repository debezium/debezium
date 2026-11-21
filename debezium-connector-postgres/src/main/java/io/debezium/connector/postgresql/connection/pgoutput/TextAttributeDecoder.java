/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import java.nio.ByteBuffer;

import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TypeRegistry;

/**
 * An attribute decoder that decodes PgOutput text encoded attributes
 */
class TextAttributeDecoder extends AbstractAttributeDecoder {

    TextAttributeDecoder() {
    }

    @Override
    public Object decode(final ByteBuffer buffer, final String columnName, final PostgresType type, final String fullType,
                         final PostgresStreamingChangeEventSource.PgConnectionSupplier connection, final boolean includeUnknownDatatypes,
                         final TypeRegistry typeRegistry) {
        final var value = decodeString(buffer);
        return PgOutputReplicationMessage.getValue(columnName, type, fullType, value, connection, includeUnknownDatatypes,
                typeRegistry);
    }
}
