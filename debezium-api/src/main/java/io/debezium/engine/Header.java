/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine;

/**
 * Represents a header that contains a key and a value.
 */
public interface Header<T> {

    /**
     * Key of a header.
     */
    String getKey();

    /**
     * Value of a header.
     */
    T getValue();

}
