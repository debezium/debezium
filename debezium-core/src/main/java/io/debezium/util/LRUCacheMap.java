/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.kafka.common.cache.Cache;

import io.debezium.annotation.NotThreadSafe;

/**
 * A custom implementation of {@link org.apache.kafka.common.cache.LRUCache} that allows exposure
 * to the underlying delegate's key or values collections.
 *
 * @author Chris Cranford
 */
@NotThreadSafe
public class LRUCacheMap<K, V> implements Cache<K, V> {

    private final LinkedHashMap<K, V> delegate;

    public LRUCacheMap(int maxSize) {
        this.delegate = new LinkedHashMap<K, V>(16, 0.75F, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return this.size() > maxSize;
            }
        };
    }

    @Override
    public V get(K key) {
        return delegate.get(key);
    }

    @Override
    public void put(K key, V value) {
        delegate.put(key, value);
    }

    @Override
    public boolean remove(K key) {
        return delegate.remove(key) != null;
    }

    @Override
    public long size() {
        return delegate.size();
    }

    public Set<K> keySet() {
        return delegate.keySet();
    }

    public Collection<V> values() {
        return delegate.values();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    public V computeIfAbsent(K key, Function<K, V> mappingFunction) {
        return delegate.computeIfAbsent(key, mappingFunction);
    }
}
