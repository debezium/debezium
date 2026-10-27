/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.cockroachdb;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkBinaryHandlingModeUnnestTest;
import io.debezium.connector.jdbc.junit.jupiter.CockroachDbSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;

/**
 * Binary handling mode tests for CockroachDB, including the UNNEST batch write path, which the
 * CockroachDB dialect inherits from the PostgreSQL dialect.
 *
 * @author Minjae Lee
 */
@Tag("all")
@Tag("it")
@Tag("it-cockroachdb")
@ExtendWith(CockroachDbSinkDatabaseContextProvider.class)
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

    @Override
    protected String largeCharacterColumnType() {
        return "text";
    }
}
