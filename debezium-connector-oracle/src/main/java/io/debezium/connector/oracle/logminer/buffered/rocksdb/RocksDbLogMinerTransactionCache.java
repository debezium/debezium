/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.rocksdb;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.logminer.buffered.AbstractLogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.buffered.rocksdb.serialization.RocksDbKeySerializer;
import io.debezium.connector.oracle.logminer.buffered.rocksdb.serialization.RocksDbTransactionSerializer;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

/**
 * A {@link io.debezium.connector.oracle.logminer.buffered.LogMinerTransactionCache} that uses RocksDB as the backing store.
 *
 * @author Debezium Authors
 */
public class RocksDbLogMinerTransactionCache extends AbstractLogMinerTransactionCache<RocksDbTransaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDbLogMinerTransactionCache.class);

    // Static byte arrays for column family names to avoid repeated allocations
    private static final byte[] EVENTS_CF_NAME = "events".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ROW_ID_INDEX_CF_NAME = "rowIdIndex".getBytes(StandardCharsets.UTF_8);
    private static final byte[] TRANSACTIONS_CF_NAME = "transactions".getBytes(StandardCharsets.UTF_8);

    private RocksDB db;
    private DBOptions dbOptions; // Keep reference for proper resource management
    private WriteOptions writeOptions; // Write options with WAL disabled for performance

    // Use single column family for all transaction events (performance improvement)
    private ColumnFamilyHandle eventsColumnFamily;
    private ColumnFamilyHandle rowIdIndexColumnFamily; // Secondary index for rowId lookups
    private ColumnFamilyHandle transactionsColumnFamily; // Stores transaction metadata (username etc.)

    // Heap-backed cache for quick access to specific metadata
    private final Map<String, TreeSet<Integer>> eventIdsByTransactionId = new HashMap<>();

    // Batch flushing for better performance
    private final int batchFlushThreshold;
    private int pendingWrites = 0;

    @SuppressWarnings("resource")
    public RocksDbLogMinerTransactionCache(String dbPath, Options options, DBOptions dbOptions, int batchFlushThreshold) {
        this.dbOptions = dbOptions;
        this.batchFlushThreshold = batchFlushThreshold;
        // Disable WAL since this is a spillover cache - data can be regenerated from source
        this.writeOptions = new WriteOptions().setDisableWAL(true);
        initializeDatabase(dbPath, options, dbOptions);
        // Heap cache is ephemeral and built on-demand as transactions are added
    }

    private void initializeDatabase(String dbPath, Options options, DBOptions dbOptions) {
        try {
            List<ColumnFamilyDescriptor> existingCFDescriptors = new ArrayList<>();
            List<ColumnFamilyHandle> existingCFHandles = new ArrayList<>();

            try {
                List<byte[]> existingCFNames = RocksDB.listColumnFamilies(options, dbPath);
                if (existingCFNames.isEmpty()) {
                    existingCFDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
                }
                else {
                    for (byte[] cfName : existingCFNames) {
                        existingCFDescriptors.add(new ColumnFamilyDescriptor(cfName, new ColumnFamilyOptions()));
                    }
                }
            }
            catch (RocksDBException e) {
                existingCFDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
            }

            this.db = RocksDB.open(dbOptions, dbPath, existingCFDescriptors, existingCFHandles);

            ColumnFamilyHandle eventsHandle = null;
            ColumnFamilyHandle indexHandle = null;
            ColumnFamilyHandle txHandle = null;

            for (ColumnFamilyHandle handle : existingCFHandles) {
                byte[] name = handle.getName();
                if (java.util.Arrays.equals(name, EVENTS_CF_NAME)) {
                    eventsHandle = handle;
                }
                else if (java.util.Arrays.equals(name, ROW_ID_INDEX_CF_NAME)) {
                    indexHandle = handle;
                }
                else if (java.util.Arrays.equals(name, TRANSACTIONS_CF_NAME)) {
                    txHandle = handle;
                }
            }

            if (eventsHandle == null) {
                ColumnFamilyDescriptor eventsDescriptor = new ColumnFamilyDescriptor(EVENTS_CF_NAME, new ColumnFamilyOptions());
                eventsHandle = db.createColumnFamily(eventsDescriptor);
            }

            if (indexHandle == null) {
                ColumnFamilyDescriptor indexDescriptor = new ColumnFamilyDescriptor(ROW_ID_INDEX_CF_NAME, new ColumnFamilyOptions());
                indexHandle = db.createColumnFamily(indexDescriptor);
            }

            if (txHandle == null) {
                ColumnFamilyDescriptor txDescriptor = new ColumnFamilyDescriptor(TRANSACTIONS_CF_NAME, new ColumnFamilyOptions());
                txHandle = db.createColumnFamily(txDescriptor);
            }

            this.eventsColumnFamily = eventsHandle;
            this.rowIdIndexColumnFamily = indexHandle;
            this.transactionsColumnFamily = txHandle;
        }
        catch (RocksDBException e) {
            throw new DebeziumException("Could not open RocksDB", e);
        }
    }

    @Override
    public void addTransaction(RocksDbTransaction transaction) {
        try {
            byte[] key = transaction.getTransactionId().getBytes(StandardCharsets.UTF_8);
            byte[] value = RocksDbTransactionSerializer.serialize(transaction);
            db.put(transactionsColumnFamily, writeOptions, key, value);

            // Initialize heap cache entry for this transaction
            eventIdsByTransactionId.put(transaction.getTransactionId(), new TreeSet<>());
        }
        catch (RocksDBException e) {
            throw new DebeziumException("Could not persist transaction metadata for " + transaction.getTransactionId(), e);
        }
    }

    @Override
    public void addTransactionEvent(RocksDbTransaction transaction, int eventKey, LogMinerEvent event) {

        try {
            byte[] compositeKey = RocksDbKeySerializer.createEventKey(transaction.getTransactionId(), eventKey);
            byte[] serializedEvent = RocksDbEventSerializer.serialize(event);

            // Log composite key as hex to avoid non-printable character pollution in logs
            LOGGER.debug("Adding event to RocksDB: txId={}, eventKey={}, rowId={}, compositeKeyHex={}",
                    transaction.getTransactionId(), eventKey, event.getRowId(), bytesToHex(compositeKey));

            db.put(eventsColumnFamily, writeOptions, compositeKey, serializedEvent);

            if (event.getRowId() != null) {
                byte[] indexKey = RocksDbKeySerializer.createRowIdIndexKey(transaction.getTransactionId(), event.getRowId());
                byte[] eventKeyBytes = RocksDbKeySerializer.intToBytes(eventKey);
                db.put(rowIdIndexColumnFamily, writeOptions, indexKey, eventKeyBytes);
                LOGGER.debug("Added secondary index: txId={}, rowId={}, eventKey={}",
                        transaction.getTransactionId(), event.getRowId(), eventKey);
            }

            // Update heap cache with event ID
            eventIdsByTransactionId.get(transaction.getTransactionId()).add(eventKey);

            pendingWrites++;
            if (pendingWrites >= batchFlushThreshold) {
                db.flush(new FlushOptions(), eventsColumnFamily);
                db.flush(new FlushOptions(), rowIdIndexColumnFamily);
                pendingWrites = 0;
                LOGGER.debug("Performed batch flush after {} writes", batchFlushThreshold);
            }

            // Persist updated transaction metadata for correct event count on restore
            try {
                byte[] txKey = transaction.getTransactionId().getBytes(StandardCharsets.UTF_8);
                byte[] txValue = RocksDbTransactionSerializer.serialize(transaction);
                db.put(transactionsColumnFamily, writeOptions, txKey, txValue);
            }
            catch (RocksDBException e) {
                LOGGER.warn("Failed to persist updated transaction metadata for {}: {}", transaction.getTransactionId(), e.getMessage());
            }

        }
        catch (RocksDBException e) {
            throw new DebeziumException("Could not add event to transaction " + transaction.getTransactionId(), e);
        }
    }

    @Override
    public boolean containsTransactionEvent(RocksDbTransaction transaction, int eventKey) {
        // Use heap cache for fast lookup
        final var events = eventIdsByTransactionId.get(transaction.getTransactionId());
        if (events != null) {
            return events.contains(eventKey);
        }
        return false;
    }

    @Override
    public LogMinerEvent getTransactionEvent(RocksDbTransaction transaction, int eventKey) {
        try {
            byte[] compositeKey = RocksDbKeySerializer.createEventKey(transaction.getTransactionId(), eventKey);
            byte[] value = db.get(eventsColumnFamily, compositeKey);
            return value != null ? RocksDbEventSerializer.deserialize(value) : null;
        }
        catch (RocksDBException e) {
            LOGGER.warn("Failed to read event {} for transaction {} from RocksDB", eventKey, transaction.getTransactionId(), e);
            // Return null and let the caller decide how to handle a missing event (like Chronicle)
            return null;
        }
    }

    @Override
    public void forEachEvent(RocksDbTransaction transaction, InterruptiblePredicate<LogMinerEvent> predicate) throws InterruptedException {
        // Use heap cache for efficient iteration
        final var events = eventIdsByTransactionId.get(transaction.getTransactionId());
        if (events != null) {
            try (var stream = events.stream()) {
                final java.util.Iterator<Integer> iterator = stream.iterator();
                while (iterator.hasNext()) {
                    final LogMinerEvent event = getTransactionEvent(transaction, iterator.next());
                    if (event != null && !predicate.test(event)) {
                        break;
                    }
                }
            }
        }
    }

    @Override
    public void removeTransaction(RocksDbTransaction transaction) {
        String txId = transaction.getTransactionId();

        try {
            byte[] prefix = txId.getBytes(StandardCharsets.UTF_8);

            byte[] endKey = new byte[prefix.length];
            System.arraycopy(prefix, 0, endKey, 0, prefix.length);
            endKey[endKey.length - 1] = (byte) (endKey[endKey.length - 1] + 1);

            db.deleteRange(eventsColumnFamily, prefix, endKey);

            db.deleteRange(rowIdIndexColumnFamily, prefix, endKey);

            db.delete(transactionsColumnFamily, prefix);

            // Remove from heap cache
            eventIdsByTransactionId.remove(txId);

            forceFlush();
        }
        catch (RocksDBException e) {
            throw new DebeziumException("Could not remove transaction " + txId, e);
        }
    }

    @Override
    public void clear() {
        try (RocksIterator iterator = db.newIterator(transactionsColumnFamily)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                RocksDbTransaction transaction = RocksDbTransactionSerializer.deserialize(iterator.value());
                removeTransaction(transaction);
            }
        }
        // Clear heap cache
        eventIdsByTransactionId.clear();
    }

    @Override
    public boolean isEmpty() {
        // Use heap cache for fast empty check
        return eventIdsByTransactionId.isEmpty();
    }

    @Override
    public int getTransactionCount() {
        // Use heap cache for fast count
        return eventIdsByTransactionId.size();
    }

    @Override
    public <R> R streamTransactionsAndReturn(Function<Stream<RocksDbTransaction>, R> consumer) {
        try (RocksIterator iterator = db.newIterator(transactionsColumnFamily)) {
            iterator.seekToFirst();
            Stream<RocksDbTransaction> stream = StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(
                            new RocksDbTransactionIterator(iterator),
                            Spliterator.ORDERED),
                    false);
            return consumer.apply(stream);
        }
    }

    @Override
    public void transactions(Consumer<Stream<RocksDbTransaction>> consumer) {
        try (RocksIterator iterator = db.newIterator(transactionsColumnFamily)) {
            iterator.seekToFirst();
            Stream<RocksDbTransaction> stream = StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(
                            new RocksDbTransactionIterator(iterator),
                            Spliterator.ORDERED),
                    false);
            consumer.accept(stream);
        }
    }

    @Override
    public RocksDbTransaction getTransaction(String transactionId) {
        try {
            byte[] key = transactionId.getBytes(StandardCharsets.UTF_8);
            byte[] data = db.get(transactionsColumnFamily, key);
            if (data != null) {
                return RocksDbTransactionSerializer.deserialize(data);
            }
            return null;
        }
        catch (RocksDBException e) {
            throw new DebeziumException("Failed to restore transaction metadata for " + transactionId, e);
        }
    }

    @Override
    public RocksDbTransaction getAndRemoveTransaction(String transactionId) {
        RocksDbTransaction transaction = getTransaction(transactionId);
        if (transaction != null) {
            removeTransaction(transaction);
        }
        return transaction;
    }

    @Override
    public boolean containsTransaction(String transactionId) {
        // Use heap cache for fast containment check
        return eventIdsByTransactionId.containsKey(transactionId);
    }

    @Override
    public void eventKeys(Consumer<Stream<String>> consumer) {
        Stream<String> eventKeyStream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        new RocksDbTransactionIterator(db.newIterator(transactionsColumnFamily)),
                        Spliterator.ORDERED),
                false)
                .flatMap(transaction -> {
                    String transactionId = transaction.getTransactionId();

                    List<String> transactionEventKeys = new ArrayList<>();
                    try (RocksIterator iterator = db.newIterator(eventsColumnFamily)) {
                        byte[] prefix = transactionId.getBytes(StandardCharsets.UTF_8);
                        iterator.seek(prefix);

                        while (iterator.isValid()) {
                            byte[] key = iterator.key();
                            if (!startsWith(key, prefix)) {
                                break; // No more keys for this transaction
                            }

                            int eventKey = ByteBuffer.wrap(key, prefix.length, 4).getInt();
                            transactionEventKeys.add(transaction.getEventId(eventKey));

                            iterator.next();
                        }
                    }
                    return transactionEventKeys.stream();
                });
        consumer.accept(eventKeyStream);
    }

    @Override
    public void removeTransactionEvents(RocksDbTransaction transaction) {
        // Remove from heap cache
        eventIdsByTransactionId.remove(transaction.getTransactionId());
        removeTransaction(transaction);
    }

    @Override
    public boolean removeTransactionEventWithRowId(RocksDbTransaction transaction, String rowId) {
        // Use heap cache to find events efficiently
        final TreeSet<Integer> eventIds = eventIdsByTransactionId.get(transaction.getTransactionId());
        if (eventIds == null) {
            return false;
        }

        try {
            // Iterate in descending order to find the latest event (like Ehcache does)
            for (Integer eventId : eventIds.descendingSet()) {
                byte[] compositeKey = RocksDbKeySerializer.createEventKey(transaction.getTransactionId(), eventId);
                byte[] eventData = db.get(eventsColumnFamily, compositeKey);

                if (eventData != null) {
                    final LogMinerEvent event = RocksDbEventSerializer.deserialize(eventData);
                    if (event != null && event.getRowId().equals(rowId)) {
                        db.delete(eventsColumnFamily, writeOptions, compositeKey);

                        byte[] indexKey = RocksDbKeySerializer.createRowIdIndexKey(transaction.getTransactionId(), rowId);
                        db.delete(rowIdIndexColumnFamily, writeOptions, indexKey);

                        // Remove from heap cache
                        eventIds.remove(eventId);
                        return true;
                    }
                }
            }

            return false;
        }
        catch (RocksDBException e) {
            throw new DebeziumException("Could not remove event from transaction " + transaction.getTransactionId(), e);
        }
    }

    @Override
    public boolean removeTransactionEventWithEventKey(RocksDbTransaction transaction, int eventKey) {
        // Check heap cache first
        final var eventIds = eventIdsByTransactionId.get(transaction.getTransactionId());
        if (eventIds != null && eventIds.contains(eventKey)) {
            try {
                byte[] compositeKey = RocksDbKeySerializer.createEventKey(transaction.getTransactionId(), eventKey);

                // Get the event data to clean up index
                byte[] existingData = db.get(eventsColumnFamily, compositeKey);

                if (existingData != null) {
                    db.delete(eventsColumnFamily, writeOptions, compositeKey);

                    try {
                        LogMinerEvent event = RocksDbEventSerializer.deserialize(existingData);
                        if (event.getRowId() != null) {
                            byte[] indexKey = RocksDbKeySerializer.createRowIdIndexKey(transaction.getTransactionId(), event.getRowId());
                            db.delete(rowIdIndexColumnFamily, writeOptions, indexKey);
                        }
                    }
                    catch (Exception e) {
                        LOGGER.warn("Failed to deserialize event for index cleanup: txId={}, eventKey={}",
                                transaction.getTransactionId(), eventKey, e);
                    }

                    // Remove from heap cache (Ehcache optimization)
                    eventIds.remove(eventKey);

                    LOGGER.info("Removed event from RocksDB: txId={}, eventKey={}, compositeKey={}",
                            transaction.getTransactionId(), eventKey, new String(compositeKey, StandardCharsets.UTF_8));
                    return true;
                }
            }
            catch (RocksDBException e) {
                throw new DebeziumException("Could not remove event with key " + eventKey + " from transaction " + transaction.getTransactionId(), e);
            }
        }
        return false;
    }

    @Override
    public int getTransactionEventCount(RocksDbTransaction transaction) {
        // Use heap cache for fast count
        final var events = eventIdsByTransactionId.get(transaction.getTransactionId());
        if (events != null) {
            return events.size();
        }
        return 0;
    }

    @Override
    public int getTransactionEvents() {
        // Use heap cache for efficient total count
        return eventIdsByTransactionId.values().stream().mapToInt(Set::size).sum();
    }

    @Override
    public void syncTransaction(RocksDbTransaction transaction) {
    }

    public void close() {
        forceFlush();

        if (writeOptions != null) {
            writeOptions.close();
        }
        if (eventsColumnFamily != null) {
            eventsColumnFamily.close();
        }
        if (rowIdIndexColumnFamily != null) {
            rowIdIndexColumnFamily.close();
        }
        if (transactionsColumnFamily != null) {
            transactionsColumnFamily.close();
        }
        if (db != null) {
            db.close();
        }
        if (dbOptions != null) {
            dbOptions.close();
        }
    }

    /**
     * Force flush any pending writes to disk.
     */
    private void forceFlush() {
        if (pendingWrites > 0) {
            try {
                db.flush(new FlushOptions(), eventsColumnFamily);
                db.flush(new FlushOptions(), rowIdIndexColumnFamily);
                LOGGER.debug("Force flushed {} pending writes", pendingWrites);
                pendingWrites = 0;
            }
            catch (RocksDBException e) {
                LOGGER.warn("Failed to force flush pending writes", e);
            }
        }
    }

    private boolean startsWith(byte[] array, byte[] prefix) {
        if (array.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (array[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }

    /**
     * Get the RocksDB instance for creating additional caches.
     *
     * @return the RocksDB instance
     */
    public RocksDB getDb() {
        return db;
    }

    /**
     * Create a column family for use by other caches.
     *
     * @param name the column family name
     * @return the column family handle
     */
    public ColumnFamilyHandle createColumnFamily(String name) {
        try {
            ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(
                    name.getBytes(StandardCharsets.UTF_8),
                    new ColumnFamilyOptions());
            return db.createColumnFamily(descriptor);
        }
        catch (RocksDBException e) {
            throw new DebeziumException("Failed to create column family: " + name, e);
        }
    }

    /**
     * A simple iterator over RocksDB transactions for streaming.
     */
    private static class RocksDbTransactionIterator implements java.util.Iterator<RocksDbTransaction> {
        private final RocksIterator rocksIterator;

        RocksDbTransactionIterator(RocksIterator rocksIterator) {
            this.rocksIterator = rocksIterator;
        }

        @Override
        public boolean hasNext() {
            return rocksIterator.isValid();
        }

        @Override
        public RocksDbTransaction next() {
            byte[] value = rocksIterator.value();
            rocksIterator.next();
            return RocksDbTransactionSerializer.deserialize(value);
        }
    }
}
