/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.sqlserver;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkBinaryHandlingModeTest;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SqlServerSinkDatabaseContextProvider;

/**
 * Binary handling mode tests for MS SQL Server.
 *
 * @author Minjae Lee
 */
@Tag("all")
@Tag("it")
@Tag("it-sqlserver")
@ExtendWith(SqlServerSinkDatabaseContextProvider.class)
public class JdbcSinkBinaryHandlingModeIT extends AbstractJdbcSinkBinaryHandlingModeTest {

    public JdbcSinkBinaryHandlingModeIT(Sink sink) {
        super(sink);
    }

    @Override
    protected String characterColumnType() {
        return "varchar(max)";
    }

    @Override
    protected String binaryColumnType() {
        return "varbinary(16)";
    }
}
