/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine;

import java.util.Collections;
import java.util.List;

/**
 * A data change event with key, value, and headers.
 *
 * @param <K>
 * @param <V>
 */
public interface ChangeEvent<K, V> {

    K key();

    V value();

    default <H> List<Header<H>> headers() {
        return Collections.emptyList();
    }

    /**
     * @return A name of the logical destination for which the event is intended
     */
    String destination();
}
