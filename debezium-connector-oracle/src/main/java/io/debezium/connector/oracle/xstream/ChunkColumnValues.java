/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import io.debezium.DebeziumException;

import oracle.sql.RAW;
import oracle.streams.ChunkColumnValue;

/**
 * A simple wrapper class around a collection of {@link ChunkColumnValue}s.
 *
 * @author Chris Cranford
 */
public class ChunkColumnValues {

    private final List<ChunkColumnValue> values = new ArrayList<>();
    private long size = 0;

    /**
     * Gets the chunk type of the managed {@link ChunkColumnValue} instances.
     *
     * @return the chunk type of the values
     * @throws DebeziumException if the method is called before adding at least one ChunkColumnValue.
     */
    public int getChunkType() {
        if (values.isEmpty()) {
            throw new DebeziumException("Unable to resolve chunk type since no chunks have yet been added.");
        }
        return values.get(0).getChunkType();
    }

    /**
     * @return {@code true} if there are no values, {@code false} if at least one value has been added.
     */
    public boolean isEmpty() {
        return values.isEmpty();
    }

    /**
     * Adds a chunk column value instance to this collection.
     *
     * @param chunkColumnValue the chunk column value to be added
     */
    public void add(ChunkColumnValue chunkColumnValue) {
        size += calculateChunkSize(chunkColumnValue);
        values.add(chunkColumnValue);
    }

    /**
     * @return the chunk data as a string, may be {@code null} if the length of the data is zero.
     * @throws SQLException if there is a database exception accessing the raw chunk value
     */
    public String getStringValue() throws SQLException {
        if (size == 0) {
            return null;
        }
        StringBuilder data = new StringBuilder();
        for (ChunkColumnValue value : values) {
            data.append(value.getColumnData().stringValue());
        }
        return data.toString();
    }

    /**
     * @return the chunk data as XML, may be {@code null} if the length of the data is zero.
     * @throws SQLException if there is a database exception accessing the raw chunk value
     */
    public String getXmlValue() throws SQLException {
        if (size == 0) {
            return null;
        }
        StringBuilder data = new StringBuilder();
        for (ChunkColumnValue value : values) {
            data.append(new String(RAW.hexString2Bytes(value.getColumnData().stringValue()), StandardCharsets.UTF_8));
        }
        return data.toString();
    }

    /**
     * @return the chunk data as a byte array, may be {@code null} if the length of the data is zero.
     * @throws SQLException if there is a database exception accessing the raw chunk value
     */
    public byte[] getByteArray() throws SQLException {
        if (size == 0) {
            if (!values.isEmpty()) {
                ChunkColumnValue firstChunk = values.get(0);
                if (firstChunk.isEmptyChunk()) {
                    return ByteBuffer.allocate(0).array();
                }
            }
            return null;
        }
        if (size > Integer.MAX_VALUE) {
            throw new DebeziumException("Size " + size + " exceeds maximum value " + Integer.MAX_VALUE);
        }
        ByteBuffer buffer = ByteBuffer.allocate((int) size);
        for (ChunkColumnValue columnValue : values) {
            buffer.put(columnValue.getColumnData().getBytes());
        }
        return buffer.array();
    }

    /**
     * Calculates the size of the individual column chunk.
     *
     * @param chunkColumnValue a specific chunk of column data
     * @return the size of the column chunk data
     * @throws DebeziumException if there was a problem resolving the size of the column chunk data
     */
    private int calculateChunkSize(ChunkColumnValue chunkColumnValue) {
        try {
            switch (chunkColumnValue.getChunkType()) {
                case ChunkColumnValue.CLOB:
                case ChunkColumnValue.NCLOB:
                case ChunkColumnValue.XMLTYPE:
                    return chunkColumnValue.getColumnData().stringValue().length();
                case ChunkColumnValue.BLOB:
                    return chunkColumnValue.getColumnData().getBytes().length;
                default:
                    return 0;
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to calculate the size of the chunk column data value");
        }
    }
}
