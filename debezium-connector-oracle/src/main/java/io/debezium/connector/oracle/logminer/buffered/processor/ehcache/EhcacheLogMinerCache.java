/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.processor.ehcache;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.ehcache.Cache;

import io.debezium.connector.oracle.logminer.buffered.processor.LogMinerCache;

/**
 * An implementation of {@link LogMinerCache} for Ehcache backed off-heap caches.
 *
 * @author Chris Cranford
 */
public class EhcacheLogMinerCache<K, V> implements LogMinerCache<K, V> {

    private final Cache<K, V> cache;
    private final String cacheName;
    private final EhcacheEvictionListener evictionListener;

    public EhcacheLogMinerCache(Cache<K, V> cache, String cacheName, EhcacheEvictionListener evictionListener) {
        this.cache = cache;
        this.cacheName = cacheName;
        this.evictionListener = evictionListener;
    }

    @Override
    public void entries(Consumer<Stream<Entry<K, V>>> streamConsumer) {
        streamConsumer.accept(getCacheStream().map(e -> new Entry<>(e.getKey(), e.getValue())));
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public V get(K key) {
        return cache.get(key);
    }

    @Override
    public boolean isEmpty() {
        // todo: how to improve this
        return size() == 0;
    }

    @Override
    public boolean containsKey(K key) {
        return cache.containsKey(key);
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, value);
        if (evictionListener.hasEvictionBeenSeen()) {
            throw new CacheCapacityExceededException(cacheName);
        }
    }

    @Override
    public V remove(K key) {
        // Ehcache does not support remove returning the existing value; therefore directly fetch it first
        V value = get(key);
        cache.remove(key);
        return value;
    }

    @Override
    public int size() {
        final int[] count = { 0 };
        cache.spliterator().forEachRemaining(element -> count[0]++);
        return count[0];
    }

    @Override
    public void forEach(BiConsumer<K, V> action) {
        getCacheStream().forEach(e -> action.accept(e.getKey(), e.getValue()));
    }

    @Override
    public void removeIf(Predicate<Entry<K, V>> predicate) {
        final List<K> keysToRemove = new ArrayList<>();
        forEach((k, v) -> {
            if (predicate.test(new Entry<>(k, v))) {
                keysToRemove.add(k);
            }
        });
        keysToRemove.forEach(cache::remove);
    }

    @Override
    public <T> T streamAndReturn(Function<Stream<Entry<K, V>>, T> function) {
        return function.apply(getCacheStream().map(e -> new Entry<>(e.getKey(), e.getValue())));
    }

    private Stream<Cache.Entry<K, V>> getCacheStream() {
        return StreamSupport.stream(cache.spliterator(), false);
    }
}
