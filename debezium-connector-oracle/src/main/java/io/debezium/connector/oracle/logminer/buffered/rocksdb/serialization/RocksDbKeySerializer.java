/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.rocksdb.serialization;

import java.nio.charset.StandardCharsets;

/**
 * Utility class for creating byte[] keys for use with RocksDB.
 */
public final class RocksDbKeySerializer {

    private RocksDbKeySerializer() {
    }

    public static byte[] createEventKey(String transactionId, int eventKey) {
        byte[] transactionIdBytes = transactionId.getBytes(StandardCharsets.UTF_8);
        byte[] eventKeyBytes = intToBytes(eventKey);
        byte[] compositeKey = new byte[transactionIdBytes.length + eventKeyBytes.length];
        System.arraycopy(transactionIdBytes, 0, compositeKey, 0, transactionIdBytes.length);
        System.arraycopy(eventKeyBytes, 0, compositeKey, transactionIdBytes.length, eventKeyBytes.length);
        return compositeKey;
    }

    public static byte[] createRowIdIndexKey(String transactionId, String rowId) {
        byte[] transactionIdBytes = transactionId.getBytes(StandardCharsets.UTF_8);
        byte[] rowIdBytes = rowId.getBytes(StandardCharsets.UTF_8);
        byte[] indexKey = new byte[transactionIdBytes.length + rowIdBytes.length];
        System.arraycopy(transactionIdBytes, 0, indexKey, 0, transactionIdBytes.length);
        System.arraycopy(rowIdBytes, 0, indexKey, transactionIdBytes.length, rowIdBytes.length);
        return indexKey;
    }

    public static byte[] intToBytes(int value) {
        return new byte[]{
                (byte) (value >>> 24),
                (byte) (value >>> 16),
                (byte) (value >>> 8),
                (byte) value
        };
    }
}
