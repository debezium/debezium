/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.ehcache.serialization;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;

/**
 * A reader implementation enabling the reading of data from an {@link InputStream}.
 *
 * @author Chris Cranford
 */
public class SerializerInputStream extends AbstractSerializerStream {

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
     * Reads an object array from the stream.
     *
     * @return the object array, never {@code null}
     * @throws IOException when a read operation fails
     */
    public Object[] readObjectArray() throws IOException {
        return stringArrayToObjectArray(readStringArray());
    }

    /**
     * Reads a string array from the stream.
     *
     * @return the string array, never {@code null}
     * @throws IOException when a read operation fails
     */
    protected String[] readStringArray() throws IOException {
        final int size = readInt();
        final String[] data = new String[size];
        for (int i = 0; i < size; i++) {
            data[i] = readString();
        }
        return data;
    }

    /**
     * Convert an array of string values, typically from the encoded stream back into their
     * Java object representations after being deserialized.
     *
     * @param values array of string values to convert
     * @return array of objects after conversion
     */
    protected Object[] stringArrayToObjectArray(String[] values) {
        Objects.requireNonNull(values);
        Object[] results = Arrays.copyOf(values, values.length, Object[].class);
        for (int i = 0; i < results.length; ++i) {
            if (results[i].equals(NULL_VALUE_SENTINEL)) {
                results[i] = null;
            }
            else if (results[i].equals(UNAVAILABLE_VALUE_SENTINEL)) {
                results[i] = OracleValueConverters.UNAVAILABLE_VALUE;
            }
        }
        return results;
    }
}
