/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.memory;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.debezium.connector.oracle.logminer.processor.LogMinerCache;

public class MapBasedLogMinerCache<K, V> implements LogMinerCache<K, V> {

    private final Map<K, V> map;

    public MapBasedLogMinerCache(Map<K, V> map) {
        this.map = map;
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
    public Optional<Entry<K, V>> first() {
        return map.entrySet().stream()
                .map(e -> new LogMinerCache.Entry<>(e.getKey(), e.getValue()))
                .findFirst();
    }

    @Override
    public Stream<Entry<K, V>> stream() {
        return map.entrySet().stream()
                .map(e -> new LogMinerCache.Entry<>(e.getKey(), e.getValue()));
    }

    @Override
    public void forEachAndRemove(BiFunction<K, V, Boolean> action) {
        Iterator<Map.Entry<K, V>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<K, V> next = iterator.next();
            if (action.apply(next.getKey(), next.getValue())) {
                iterator.remove();
            }
        }
    }

}
