/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.rocksdb.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.buffered.rocksdb.RocksDbEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.rocksdb.RocksDbTransaction;

/**
 * Handles serialization and deserialization for RocksDbTransaction metadata.
 *
 * This utility reuses the string helpers provided by {@link RocksDbEventSerializer.BaseEventSerializer}
 * by creating a singleton instance and delegating to the protected helpers. The class still exposes
 * static convenience methods for callers.
 */
public final class RocksDbTransactionSerializer extends RocksDbEventSerializer.BaseEventSerializer {

    private static final RocksDbTransactionSerializer INSTANCE = new RocksDbTransactionSerializer();

    private RocksDbTransactionSerializer() {
        // private singleton
    }

    /**
     * Serializes a RocksDbTransaction object into a byte array.
     * @param tx The transaction to serialize.
     * @return A byte array representing the transaction.
     */
    public static byte[] serialize(RocksDbTransaction tx) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream out = new DataOutputStream(baos)) {
            INSTANCE.writeString(out, tx.getTransactionId());
            INSTANCE.writeString(out, tx.getStartScn() == null || tx.getStartScn().isNull() ? "null" : tx.getStartScn().toString());
            INSTANCE.writeString(out, tx.getChangeTime() == null ? null : tx.getChangeTime().toString());
            INSTANCE.writeString(out, tx.getUserName());
            out.writeInt(tx.getRedoThreadId());
            out.writeInt(tx.getNumberOfEvents());
            INSTANCE.writeString(out, tx.getClientId());
            out.flush();
            return baos.toByteArray();
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to serialize transaction metadata for " + tx.getTransactionId(), e);
        }
    }

    /**
     * Deserializes a byte array back into a RocksDbTransaction object.
     * @param data The byte array to deserialize.
     * @return The reconstructed RocksDbTransaction object.
     */
    public static RocksDbTransaction deserialize(byte[] data) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data); DataInputStream in = new DataInputStream(bais)) {
            String transactionId = INSTANCE.readString(in);
            String scnString = INSTANCE.readString(in);
            Scn scn = "null".equals(scnString) ? Scn.NULL : Scn.valueOf(scnString);
            String changeTimeString = INSTANCE.readString(in);
            Instant changeTime = changeTimeString == null ? null : Instant.parse(changeTimeString);
            String userName = INSTANCE.readString(in);
            int redoThread = in.readInt();
            int numberOfEvents = in.readInt();
            String clientId = INSTANCE.readString(in);

            // Reconstruct the transaction object using the constructor that accepts the numberOfEvents
            Integer redoThreadObj = redoThread == -1 ? null : Integer.valueOf(redoThread);
            RocksDbTransaction tx = new RocksDbTransaction(transactionId, scn, changeTime, userName, redoThreadObj, numberOfEvents, clientId);
            return tx;
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to deserialize transaction metadata", e);
        }
    }

    // The class extends BaseEventSerializer only to reuse the protected helpers; the
    // RocksDbEventSerializerStrategy methods are not used by this utility
    @Override
    public byte getTypeId() {
        throw new UnsupportedOperationException("Not a RocksDbEventSerializerStrategy implementation");
    }

    @Override
    public void writeEvent(DataOutputStream out, io.debezium.connector.oracle.logminer.events.LogMinerEvent event) throws IOException {
        throw new UnsupportedOperationException("Not a RocksDbEventSerializerStrategy implementation");
    }

    @Override
    public io.debezium.connector.oracle.logminer.events.LogMinerEvent readEvent(DataInputStream in) throws IOException {
        throw new UnsupportedOperationException("Not a RocksDbEventSerializerStrategy implementation");
    }
}
