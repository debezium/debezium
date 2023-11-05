/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.sqlserver;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkSaveConvertedCloudEventTest;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SqlServerSinkDatabaseContextProvider;

/**
 * Converted CloudEvent saving tests for MS SQL Server
 *
 * @author Roman Kudryashov
 */
@Tag("all")
@Tag("it")
@Tag("it-sqlserver")
@ExtendWith(SqlServerSinkDatabaseContextProvider.class)
public class JdbcSinkSaveConvertedCloudEventIT extends AbstractJdbcSinkSaveConvertedCloudEventTest {

    public JdbcSinkSaveConvertedCloudEventIT(Sink sink) {
        super(sink);
    }

}
