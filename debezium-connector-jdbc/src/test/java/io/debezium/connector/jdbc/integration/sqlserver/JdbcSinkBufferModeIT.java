/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.sqlserver;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkBufferModeTest;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SqlServerSinkDatabaseContextProvider;

/**
 * Buffer mode integration tests for SQL Server.
 *
 * @author rk3rn3r
 */
@Tag("all")
@Tag("it")
@Tag("it-sqlserver")
@ExtendWith(SqlServerSinkDatabaseContextProvider.class)
public class JdbcSinkBufferModeIT extends AbstractJdbcSinkBufferModeTest {

    public JdbcSinkBufferModeIT(Sink sink) {
        super(sink);
    }
}
