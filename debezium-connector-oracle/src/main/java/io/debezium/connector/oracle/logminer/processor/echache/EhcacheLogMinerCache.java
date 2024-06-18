/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.echache;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.ehcache.Cache;

import io.debezium.connector.oracle.logminer.processor.LogMinerCache;

public class EhcacheLogMinerCache<K, V> implements LogMinerCache<K, V> {

    private final Cache<K, V> cache;
    private AtomicInteger size = new AtomicInteger();

    public EhcacheLogMinerCache(Cache<K, V> cache) {
        this.cache = cache;
    }

    @Override
    public void clear() {
        cache.clear();
        size.set(0);
    }

    @Override
    public int size() {
        return size.get();
    }

    @Override
    public boolean isEmpty() {
        return !cache.iterator().hasNext();
    }

    @Override
    public boolean containsKey(K key) {
        return cache.containsKey(key);
    }

    @Override
    public V remove(K key) {
        boolean decrement = containsKey(key);
        V result = cache.get(key);
        cache.remove(key);
        if (decrement) {
            size.decrementAndGet();
        }
        return result;
    }

    @Override
    public V get(K key) {
        return cache.get(key);
    }

    @Override
    public void put(K key, V value) {
        boolean increment = !containsKey(key);
        cache.put(key, value);
        if (increment) {
            size.incrementAndGet();
        }
    }

    @Override
    public void forEach(BiConsumer<K, V> action) {
        cache.forEach(entry -> action.accept(entry.getKey(), entry.getValue()));
    }

    @Override
    public void removeIf(Predicate<Entry<K, V>> predicate) {
        Iterator<Cache.Entry<K, V>> iterator = cache.iterator();
        while (iterator.hasNext()) {
            Cache.Entry<K, V> entry = iterator.next();
            V transaction = entry.getValue();
            if (predicate.test(new Entry<>(entry.getKey(), transaction))) {
                iterator.remove();
                size.decrementAndGet();
            }
        }
    }

    @Override
    public void entries(Consumer<Stream<Entry<K, V>>> streamConsumer) {
        try (Stream<Entry<K, V>> stream = StreamSupport.stream(cache.spliterator(), false)
                .map(e -> new Entry<>(e.getKey(), e.getValue()))) {
            streamConsumer.accept(stream);
        }
    }

    @Override
    public <T> T streamAndReturn(Function<Stream<Entry<K, V>>, T> function) {
        try (Stream<Entry<K, V>> stream = StreamSupport.stream(cache.spliterator(), false)
                .map(e -> new Entry<>(e.getKey(), e.getValue()))) {
            return function.apply(stream);
        }
    }
}
