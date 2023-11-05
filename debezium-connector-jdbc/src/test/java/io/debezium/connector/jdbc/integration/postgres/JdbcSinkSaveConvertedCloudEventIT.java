/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.postgres;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkSaveConvertedCloudEventTest;
import io.debezium.connector.jdbc.junit.jupiter.PostgresSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;

/**
 * Converted CloudEvent saving tests for PostgreSQL
 *
 * @author Roman Kudryashov
 */
@Tag("all")
@Tag("it")
@Tag("it-postgresql")
@ExtendWith(PostgresSinkDatabaseContextProvider.class)
public class JdbcSinkSaveConvertedCloudEventIT extends AbstractJdbcSinkSaveConvertedCloudEventTest {

    public JdbcSinkSaveConvertedCloudEventIT(Sink sink) {
        super(sink);
    }

}
