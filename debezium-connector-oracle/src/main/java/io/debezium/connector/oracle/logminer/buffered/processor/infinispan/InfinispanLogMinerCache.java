/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.processor.infinispan;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.api.BasicCache;

import io.debezium.connector.oracle.logminer.buffered.processor.LogMinerCache;

public class InfinispanLogMinerCache<K, V> implements LogMinerCache<K, V> {

    private final BasicCache<K, V> cache;

    public InfinispanLogMinerCache(BasicCache<K, V> cache) {
        this.cache = cache;
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
        if (cache instanceof RemoteCache<K, V> remoteCache) {
            return remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).remove(key);
        }
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
    public void removeIf(Predicate<Entry<K, V>> predicate) {
        this.cache.entrySet().removeIf(kvEntry -> predicate.test(new Entry<>(kvEntry.getKey(), kvEntry.getValue())));
    }

    @Override
    public void entries(Consumer<Stream<Entry<K, V>>> streamConsumer) {
        try (Stream<Entry<K, V>> stream = this.cache.entrySet()
                .stream()
                .map(e -> new Entry<>(e.getKey(), e.getValue()))) {
            streamConsumer.accept(stream);
        }
    }

    @Override
    public <T> T streamAndReturn(Function<Stream<Entry<K, V>>, T> function) {
        try (Stream<Map.Entry<K, V>> stream = this.cache.entrySet().stream()) {
            return function.apply(stream.map(e -> new Entry<>(e.getKey(), e.getValue())));
        }
    }
}
