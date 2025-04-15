/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.processor.memory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.debezium.connector.oracle.logminer.buffered.processor.LogMinerCache;

public class MemoryBasedLogMinerCache<K, V> implements LogMinerCache<K, V> {

    private final Map<K, V> map;

    public MemoryBasedLogMinerCache() {
        this.map = new HashMap<>();
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    @Override
    public V remove(K key) {
        return map.remove(key);
    }

    @Override
    public V get(K key) {
        return map.get(key);
    }

    @Override
    public void put(K key, V value) {
        map.put(key, value);
    }

    @Override
    public void forEach(BiConsumer<K, V> action) {
        map.forEach(action);
    }

    @Override
    public void removeIf(Predicate<Entry<K, V>> predicate) {
        this.map.entrySet().removeIf(kvEntry -> predicate.test(new Entry<>(kvEntry.getKey(), kvEntry.getValue())));
    }

    @Override
    public void entries(Consumer<Stream<Entry<K, V>>> streamConsumer) {
        streamConsumer.accept(map.entrySet().stream()
                .map(e -> new LogMinerCache.Entry<>(e.getKey(), e.getValue())));
    }

    @Override
    public <T> T streamAndReturn(Function<Stream<Entry<K, V>>, T> function) {
        try (Stream<Map.Entry<K, V>> stream = map.entrySet().stream()) {
            return function.apply(stream.map(e -> new Entry<>(e.getKey(), e.getValue())));
        }
    }

}
