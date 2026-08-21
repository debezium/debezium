/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.postgres;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkBinaryHandlingModeUnnestTest;
import io.debezium.connector.jdbc.junit.jupiter.PostgresSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;

/**
 * Binary handling mode tests for PostgreSQL, including the UNNEST batch write path.
 *
 * @author Minjae Lee
 */
@Tag("all")
@Tag("it")
@Tag("it-postgresql")
@ExtendWith(PostgresSinkDatabaseContextProvider.class)
public class JdbcSinkBinaryHandlingModeIT extends AbstractJdbcSinkBinaryHandlingModeUnnestTest {

    public JdbcSinkBinaryHandlingModeIT(Sink sink) {
        super(sink);
    }

    @Override
    protected String characterColumnType() {
        return "text";
    }

    @Override
    protected String binaryColumnType() {
        return "bytea";
    }
}
