/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache.serialization;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;

import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;

/**
 * @author Chris Cranford
 */
public class SerializerOutputStream implements AutoCloseable {

    private final DataOutputStream delegate;

    public SerializerOutputStream(OutputStream outputStream) {
        this.delegate = new DataOutputStream(outputStream);
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    /**
     * Writes an Oracle SCN value to the stream.
     *
     * @param scn the SCN value
     * @throws IOException when a write operation fails
     */
    public void writeScn(Scn scn) throws IOException {
        writeString(scn == null || scn.isNull() ? null : scn.toString());
    }

    /**
     * Writes a {@link TableId} to the stream.
     *
     * @param tableId the table id, should not be {@code null}
     * @throws IOException when a write operation fails
     */
    public void writeTableId(TableId tableId) throws IOException {
        Objects.requireNonNull(tableId);
        writeString(tableId.toDoubleQuotedString());
    }

    /**
     * Writes an {@link Instant} to the stream.
     * @param instant the instant to be written, should not be {@code null}
     * @throws IOException when a write operation fails
     */
    public void writeInstant(Instant instant) throws IOException {
        Objects.requireNonNull(instant);
        writeString(instant.toString());
    }

    /**
     * Writes a boolean value to the stream.
     *
     * @param value the boolean value to write
     * @throws IOException when a write operation fails
     */
    public void writeBoolean(boolean value) throws IOException {
        delegate.writeBoolean(value);
    }

    /**
     * Write an integer value to the stream.
     *
     * @param value the integer value to write
     * @throws IOException when a write operation fails
     */
    public void writeInt(int value) throws IOException {
        delegate.writeInt(value);
    }

    /**
     * Write a long value to the stream.
     *
     * @param value the long value to write
     * @throws IOException when a write operation fails
     */
    public void writeLong(long value) throws IOException {
        delegate.writeLong(value);
    }

    /**
     * Write a string value to the stream.
     *
     * @param value the string value to write, can be {@code null}
     * @throws IOException when a write operation fails
     */
    public void writeString(String value) throws IOException {
        // When writing to a DataOutputStream, the writeUTF method does not accept a null string value.
        // To account for this, Debezium will write a preceding boolean flag indicating nullability,
        // and the string will only be written if the string is not null.
        if (value == null) {
            delegate.writeBoolean(true);
        }
        else {
            delegate.writeBoolean(false);
            delegate.writeUTF(value);
        }
    }

    /**
     * Writes a string of any length as a length-prefixed UTF-8 byte array.
     * <p>
     * {@link #writeString(String)} relies on {@link java.io.DataOutputStream#writeUTF(String)}, which is capped
     * at 65,535 encoded bytes; a redo statement with its continuation rows joined can exceed that.
     *
     * @param value the string to write, can be {@code null}
     * @throws IOException when a write operation fails
     */
    public void writeLongString(String value) throws IOException {
        if (value == null) {
            delegate.writeBoolean(true);
            return;
        }
        delegate.writeBoolean(false);
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        delegate.writeInt(bytes.length);
        delegate.write(bytes);
    }
}
