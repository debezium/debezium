/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

/**
 * Provides the access to the original encapsulated value obtained for example from JDBC.
 *
 * @author Jiri Pechanec
 *
 */
public interface ValueWrapper<T> {

    T getWrappedValue();
}
