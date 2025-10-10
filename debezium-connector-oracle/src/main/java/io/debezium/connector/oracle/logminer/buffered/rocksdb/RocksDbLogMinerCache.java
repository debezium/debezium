/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.rocksdb;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.logminer.buffered.LogMinerCache;

/**
 * An implementation of {@link LogMinerCache} for RocksDB backed disk caches.
 *
 * @author Debezium Authors
 */
public class RocksDbLogMinerCache<K, V> implements LogMinerCache<K, V> {

    private final RocksDB db;
    private final ColumnFamilyHandle columnFamily;
    private final String cacheName;

    public RocksDbLogMinerCache(RocksDB db, ColumnFamilyHandle columnFamily, String cacheName) {
        this.db = db;
        this.columnFamily = columnFamily;
        this.cacheName = cacheName;
    }

    @Override
    public void entries(Consumer<Stream<Entry<K, V>>> streamConsumer) {
        List<Entry<K, V>> entries = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator(columnFamily)) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                @SuppressWarnings("unchecked")
                K key = (K) new String(iterator.key(), StandardCharsets.UTF_8);
                @SuppressWarnings("unchecked")
                V value = (V) new String(iterator.value(), StandardCharsets.UTF_8);
                entries.add(new Entry<>(key, value));
                iterator.next();
            }
        }
        streamConsumer.accept(entries.stream());
    }

    @Override
    public void clear() {
        try (RocksIterator iterator = db.newIterator(columnFamily)) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                db.delete(columnFamily, iterator.key());
                iterator.next();
            }
        }
        catch (RocksDBException e) {
            throw new DebeziumException("Failed to clear RocksDB cache: " + cacheName, e);
        }
    }

    @Override
    public V get(K key) {
        try {
            byte[] keyBytes = key.toString().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = db.get(columnFamily, keyBytes);
            if (valueBytes == null) {
                return null;
            }
            @SuppressWarnings("unchecked")
            V value = (V) new String(valueBytes, StandardCharsets.UTF_8);
            return value;
        }
        catch (RocksDBException e) {
            throw new DebeziumException("Failed to get value from RocksDB cache: " + cacheName, e);
        }
    }

    @Override
    public boolean isEmpty() {
        try (RocksIterator iterator = db.newIterator(columnFamily)) {
            iterator.seekToFirst();
            return !iterator.isValid();
        }
    }

    @Override
    public boolean containsKey(K key) {
        try {
            byte[] keyBytes = key.toString().getBytes(StandardCharsets.UTF_8);
            byte[] value = db.get(columnFamily, keyBytes);
            return value != null;
        }
        catch (RocksDBException e) {
            throw new DebeziumException("Failed to check key existence in RocksDB cache: " + cacheName, e);
        }
    }

    @Override
    public void put(K key, V value) {
        try {
            byte[] keyBytes = key.toString().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = value.toString().getBytes(StandardCharsets.UTF_8);
            db.put(columnFamily, keyBytes, valueBytes);
        }
        catch (RocksDBException e) {
            throw new DebeziumException("Failed to put value into RocksDB cache: " + cacheName, e);
        }
    }

    @Override
    public V remove(K key) {
        try {
            byte[] keyBytes = key.toString().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = db.get(columnFamily, keyBytes);
            if (valueBytes != null) {
                db.delete(columnFamily, keyBytes);
                @SuppressWarnings("unchecked")
                V value = (V) new String(valueBytes, StandardCharsets.UTF_8);
                return value;
            }
            return null;
        }
        catch (RocksDBException e) {
            throw new DebeziumException("Failed to remove value from RocksDB cache: " + cacheName, e);
        }
    }

    @Override
    public int size() {
        int count = 0;
        try (RocksIterator iterator = db.newIterator(columnFamily)) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                count++;
                iterator.next();
            }
        }
        return count;
    }

    @Override
    public void forEach(BiConsumer<K, V> action) {
        try (RocksIterator iterator = db.newIterator(columnFamily)) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                @SuppressWarnings("unchecked")
                K key = (K) new String(iterator.key(), StandardCharsets.UTF_8);
                @SuppressWarnings("unchecked")
                V value = (V) new String(iterator.value(), StandardCharsets.UTF_8);
                action.accept(key, value);
                iterator.next();
            }
        }
    }

    @Override
    public void removeIf(Predicate<Entry<K, V>> predicate) {
        List<byte[]> keysToRemove = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator(columnFamily)) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                @SuppressWarnings("unchecked")
                K key = (K) new String(iterator.key(), StandardCharsets.UTF_8);
                @SuppressWarnings("unchecked")
                V value = (V) new String(iterator.value(), StandardCharsets.UTF_8);
                if (predicate.test(new Entry<>(key, value))) {
                    keysToRemove.add(iterator.key());
                }
                iterator.next();
            }
        }

        try {
            for (byte[] keyBytes : keysToRemove) {
                db.delete(columnFamily, keyBytes);
            }
        }
        catch (RocksDBException e) {
            throw new DebeziumException("Failed to remove entries from RocksDB cache: " + cacheName, e);
        }
    }

    @Override
    public <T> T streamAndReturn(Function<Stream<Entry<K, V>>, T> function) {
        List<Entry<K, V>> entries = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator(columnFamily)) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                @SuppressWarnings("unchecked")
                K key = (K) new String(iterator.key(), StandardCharsets.UTF_8);
                @SuppressWarnings("unchecked")
                V value = (V) new String(iterator.value(), StandardCharsets.UTF_8);
                entries.add(new Entry<>(key, value));
                iterator.next();
            }
        }
        try (Stream<Entry<K, V>> stream = entries.stream()) {
            return function.apply(stream);
        }
    }
}
