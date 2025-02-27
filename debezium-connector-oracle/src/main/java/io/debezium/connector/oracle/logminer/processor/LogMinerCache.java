/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Interface describing the functionality needed to cache data in during LogMiner processing.
 */
public interface LogMinerCache<K, V> {

    /**
     * Consume a Stream of all the entries in the cache.
     * @param entryStream
     */
    void entries(Consumer<Stream<Entry<K, V>>> entryStream);

    /**
     * A Stream of available keys will be provided to the given Consumer.
     */
    default void keys(Consumer<Stream<K>> keyStreamConsumer) {
        entries(entryStream -> keyStreamConsumer.accept(entryStream.map(Entry::getKey)));
    }

    /**
     * A Stream of available values will be provided to the given Consumer.
     */
    default void values(Consumer<Stream<V>> valueStreamConsumer) {
        entries(entryStream -> valueStreamConsumer.accept(entryStream.map(Entry::getValue)));
    }

    /**
     * Clear all keys/values from the cache.
     */
    void clear();

    /**
     * Retrieve the value for the given key.
     */
    V get(K key);

    /**
     * Returns true if the cache is empty.
     */
    boolean isEmpty();

    /**
     * Returns true if the cache contains the given key.
     */
    boolean containsKey(K key);

    /**
     * Add the key and value into the cache.
     */
    void put(K key, V value);

    /**
     * Remove the given key from the cache and return the value that was associated with it.
     */
    V remove(K key);

    /**
     * Returns the size of the cache.
     */
    int size();

    void forEach(BiConsumer<K, V> action);

    /**
     * Remove all keys/values from the cache when the {@link Predicate} returns true;
     */
    void removeIf(Predicate<Entry<K, V>> predicate);

    /**
     * Apply the given function to the provided stream and return the result from the function.
     */
    <T> T streamAndReturn(Function<Stream<Entry<K, V>>, T> function);

    class Entry<K, V> {
        private final K key;
        private final V value;

        public Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }

}
