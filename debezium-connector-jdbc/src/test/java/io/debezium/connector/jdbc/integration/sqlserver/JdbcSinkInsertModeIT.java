/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.sqlserver;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkInsertModeTest;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SqlServerSinkDatabaseContextProvider;

/**
 * Insert Mode tests for MS SQL Server.
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("it")
@Tag("it-sqlserver")
@ExtendWith(SqlServerSinkDatabaseContextProvider.class)
public class JdbcSinkInsertModeIT extends AbstractJdbcSinkInsertModeTest {

    public JdbcSinkInsertModeIT(Sink sink) {
        super(sink);
    }

}
