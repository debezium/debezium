/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.cockroachdb;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkDeleteEnabledTest;
import io.debezium.connector.jdbc.junit.jupiter.CockroachDbSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;

/**
 * Delete enabled tests for CockroachDB.
 *
 * @author Virag Tripathi
 */
@Tag("all")
@Tag("it")
@Tag("it-cockroachdb")
@ExtendWith(CockroachDbSinkDatabaseContextProvider.class)
public class JdbcSinkDeleteEnabledIT extends AbstractJdbcSinkDeleteEnabledTest {

    public JdbcSinkDeleteEnabledIT(Sink sink) {
        super(sink);
    }

}
