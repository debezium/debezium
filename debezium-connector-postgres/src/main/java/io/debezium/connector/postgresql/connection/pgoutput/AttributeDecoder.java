/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import java.nio.ByteBuffer;

import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TypeRegistry;

/**
 * Postgres attribute (column) decoder. The decodes a value from the wire format (e.g. UTF-8 encoded text or binary) to corresponding
 * java type.
 */
interface AttributeDecoder {

    /**
     * Decodes
     *
     * @param buffer                  the buffer returned from Postgres JDBC driver. The method assumes the buffer is a heap buffer and the position of the
     *                                buffer is on the start of the encoded attribute value.
     * @param columnName              the column name.
     * @param type                    Postgres type of the column.
     * @param fullType                the full type-name of the column
     * @param connection              Postgres connection supplier.
     * @param includeUnknownDatatypes if the decoder should include unknown data types
     * @param typeRegistry            the Postgres type registry.
     * @return decoded Java object
     */
    Object decode(ByteBuffer buffer, String columnName, PostgresType type, String fullType,
                  PgConnectionSupplier connection, boolean includeUnknownDatatypes, TypeRegistry typeRegistry);

    /**
     * Create a new text attribute decoder.
     *
     * @return a new text attribute decoder.
     */
    static AttributeDecoder ofText() {
        return new TextAttributeDecoder();
    }

    /**
     * Create a new binary attribute decoder.
     *
     * @return a new binary attribute decoder.
     */
    static AttributeDecoder ofBinary() {
        return new BinaryAttributeDecoder();
    }
}
