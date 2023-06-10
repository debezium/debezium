/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.postgres;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkSchemaEvolutionTest;
import io.debezium.connector.jdbc.junit.jupiter.PostgresSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;

/**
 * Schema Evolution tests for PostgreSQL
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("it")
@Tag("it-postgresql")
@ExtendWith(PostgresSinkDatabaseContextProvider.class)
public class JdbcSinkSchemaEvolutionIT extends AbstractJdbcSinkSchemaEvolutionTest {

    public JdbcSinkSchemaEvolutionIT(Sink sink) {
        super(sink);
    }

    @Override
    protected String getDatabaseSchemaName() {
        return "public";
    }
}
