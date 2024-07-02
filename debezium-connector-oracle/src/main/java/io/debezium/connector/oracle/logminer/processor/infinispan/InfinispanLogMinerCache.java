/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.infinispan;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.util.CloseableIterator;

import io.debezium.connector.oracle.logminer.processor.LogMinerCache;

public class InfinispanLogMinerCache<K, V> implements LogMinerCache<K, V> {

    private final BasicCache<K, V> cache;
    private final boolean closeable;

    public InfinispanLogMinerCache(BasicCache<K, V> cache) {
        this.cache = cache;
        this.closeable = cache instanceof Cache;
    }

    @Override
    public boolean containsKey(K key) {
        return cache.containsKey(key);
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public boolean isEmpty() {
        return cache.isEmpty();
    }

    @Override
    public V remove(K key) {
        return cache.remove(key);
    }

    @Override
    public V get(K key) {
        return cache.get(key);
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, value);
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public void forEach(BiConsumer<K, V> action) {
        cache.forEach(action);
    }

    @Override
    public void forEachAndRemove(BiFunction<K, V, Boolean> action) {
        Iterator<Map.Entry<K, V>> iterator = null;
        try {
            iterator = cache.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<K, V> entry = iterator.next();
                if (Boolean.TRUE.equals(action.apply(entry.getKey(), entry.getValue()))) {
                    iterator.remove();
                }
            }
        }
        finally {
            if (closeable && (iterator != null)) {
                ((CloseableIterator<?>) iterator).close();
            }
        }
    }

    @Override
    public void removeIf(Predicate<Entry<K, V>> predicate) {
        this.cache.entrySet().removeIf(kvEntry -> predicate.test(new Entry<>(kvEntry.getKey(), kvEntry.getValue())));
    }

    @Override
    public Optional<Entry<K, V>> first() {
        try (Stream<Map.Entry<K, V>> stream = this.cache.entrySet().stream()) {
            return stream.map(e -> new LogMinerCache.Entry<>(e.getKey(), e.getValue())).findFirst();
        }
    }

    @Override
    public void stream(Consumer<Stream<Entry<K, V>>> streamConsumer) {
        try (Stream<Entry<K, V>> stream = this.cache.entrySet()
                .stream()
                .map(e -> new Entry<>(e.getKey(), e.getValue()))) {
            streamConsumer.accept(stream);
        }
    }
}
