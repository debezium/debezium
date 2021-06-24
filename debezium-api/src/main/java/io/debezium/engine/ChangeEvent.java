/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine;

import io.debezium.common.annotation.Incubating;

/**
 * A data change event with key and value.
 *
 * @param <K>
 * @param <V>
 */
@Incubating
public interface ChangeEvent<K, V> {

    public K key();

    public V value();

    /**
     * @return A name of the logical destination for which the event is intended
     */
    public String destination();
}
