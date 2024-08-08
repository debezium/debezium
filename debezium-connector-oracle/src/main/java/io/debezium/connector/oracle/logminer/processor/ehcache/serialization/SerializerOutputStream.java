/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.ehcache.serialization;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Objects;

import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.relational.TableId;

/**
 * @author Chris Cranford
 */
public class SerializerOutputStream extends AbstractSerializerStream {

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
     * Write an object array to the stream.
     *
     * @param values the object array to write, should not be {@code null}
     * @throws IOException when a write operation fails
     */
    public void writeObjectArray(Object[] values) throws IOException {
        Objects.requireNonNull(values);
        writeStringArray(objectArrayToStringArray(values));
    }

    /**
     * Writes a string array to the stream.
     *
     * @param values the string array to write, should not be {@code null}
     * @throws IOException when a write operation fails
     */
    protected void writeStringArray(String[] values) throws IOException {
        Objects.requireNonNull(values);
        writeInt(values.length);
        for (String value : values) {
            writeString(value);
        }
    }

    /**
     * Convert an array of object values to a string array for serialization. This is typically
     * used when preparing the {@link LogMinerDmlEntry} values array for serialization.
     *
     * @param values the dml entry's new or old object values array
     * @return string array of values prepared for serialization
     */
    protected String[] objectArrayToStringArray(Object[] values) {
        Objects.requireNonNull(values);
        String[] results = new String[values.length];
        for (int i = 0; i < values.length; ++i) {
            if (values[i] == null) {
                results[i] = NULL_VALUE_SENTINEL;
            }
            else if (values[i] == OracleValueConverters.UNAVAILABLE_VALUE) {
                results[i] = UNAVAILABLE_VALUE_SENTINEL;
            }
            else {
                results[i] = (String) values[i];
            }
        }
        return results;
    }
}
