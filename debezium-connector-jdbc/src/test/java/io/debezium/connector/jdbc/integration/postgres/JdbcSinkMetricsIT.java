/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.postgres;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkMetricsTest;
import io.debezium.connector.jdbc.junit.jupiter.PostgresSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;

/**
 * JMX metrics tests for MySQL.
 *
 */
@Tag("all")
@Tag("it")
@Tag("it-postgres")
@ExtendWith(PostgresSinkDatabaseContextProvider.class)
public class JdbcSinkMetricsIT extends AbstractJdbcSinkMetricsTest {

    public JdbcSinkMetricsIT(Sink sink) {
        super(sink);
    }

}
