/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public interface LogMinerCache<K, V> {
    default void keys(Consumer<Stream<K>> keyStream) {
        stream(entryStream -> keyStream.accept(entryStream.map(Entry::getKey)));
    }

    default void values(Consumer<Stream<V>> valueStream) {
        stream(entryStream -> valueStream.accept(entryStream.map(Entry::getValue)));
    }

    boolean isEmpty();

    boolean containsKey(K key);

    int size();

    V remove(K key);

    V get(K key);

    void put(K key, V value);

    void clear();

    void forEach(BiConsumer<K, V> action);

    void forEachAndRemove(BiFunction<K, V, Boolean> action);

    void removeIf(Predicate<Entry<K, V>> predicate);

    Optional<Entry<K, V>> first();

    void stream(Consumer<Stream<Entry<K, V>>> entryStream);

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
