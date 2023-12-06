/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors.spi;

import io.debezium.jdbc.JdbcConnection;

/**
 * Contract that allows injecting a {@link JdbcConnection}.
 *
 * @author Chris Cranford
 */
public interface JdbcConnectionAware {
    void setDatabaseConnection(JdbcConnection connection);
}
