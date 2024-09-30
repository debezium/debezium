/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.mysql;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkSchemaEvolutionTest;
import io.debezium.connector.jdbc.junit.jupiter.MySqlSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;

/**
 * Schema Evolution tests for MySQL
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("it")
@Tag("it-mysql")
@ExtendWith(MySqlSinkDatabaseContextProvider.class)
public class JdbcSinkSchemaEvolutionIT extends AbstractJdbcSinkSchemaEvolutionTest {

    public JdbcSinkSchemaEvolutionIT(Sink sink) {
        super(sink);
    }

}
