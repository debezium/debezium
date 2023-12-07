/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors.spi;

/**
 * Contract that allows injecting a database connection.
 *
 * @author Chris Cranford
 */
public interface ConnectionAware<T> {
    void setDatabaseConnection(T connection);
}
