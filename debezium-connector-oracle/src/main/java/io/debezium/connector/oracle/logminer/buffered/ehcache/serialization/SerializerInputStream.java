/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache.serialization;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;

/**
 * A reader implementation enabling the reading of data from an {@link InputStream}.
 *
 * @author Chris Cranford
 */
public class SerializerInputStream implements AutoCloseable {

    private final DataInputStream delegate;

    public SerializerInputStream(InputStream inputStream) {
        this.delegate = new DataInputStream(inputStream);
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    /**
     * Reads an Oracle SCN value from the stream.
     *
     * @return the read SCN value or {@link Scn#NULL} if null
     * @throws IOException when a read operation fails
     */
    public Scn readScn() throws IOException {
        final String value = readString();
        return value == null ? Scn.NULL : Scn.valueOf(value);
    }

    /**
     * Reads a Debezium {@link TableId} from the stream.
     *
     * @return the table id, never {@code null}
     * @throws IOException when a read operation fails
     */
    public TableId readTableId() throws IOException {
        return TableId.parse(readString());
    }

    /**
     * Read an {@link Instant} from the stream.
     *
     * @return the read instant value, never {@code null}
     * @throws IOException when a read operation fails
     */
    public Instant readInstant() throws IOException {
        return Instant.parse(readString());
    }

    /**
     * Read a boolean value from the stream.
     *
     * @return the boolean value
     * @throws IOException when a read operation fails
     */
    public boolean readBoolean() throws IOException {
        return delegate.readBoolean();
    }

    /**
     * Read an integer value from the stream.
     *
     * @return the integer value
     * @throws IOException when a read operation fails
     */
    public int readInt() throws IOException {
        return delegate.readInt();
    }

    /**
     * Read a long value from the stream.
     *
     * @return the long value
     * @throws IOException when a read operation fails
     */
    public long readLong() throws IOException {
        return delegate.readLong();
    }

    /**
     * Read a string value from the stream.
     *
     * @return the string value or {@code null} when null
     * @throws IOException when a read operation fails
     */
    public String readString() throws IOException {
        // Strings are serialized with a boolean flag preceding the string indicating nullability.
        boolean isNull = delegate.readBoolean();
        if (isNull) {
            return null;
        }
        return delegate.readUTF();
    }

    /**
     * Reads a string written by {@link SerializerOutputStream#writeLongString(String)}.
     *
     * @return the string value or {@code null} when null
     * @throws IOException when a read operation fails
     */
    public String readLongString() throws IOException {
        if (delegate.readBoolean()) {
            return null;
        }
        final byte[] bytes = new byte[delegate.readInt()];
        delegate.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
