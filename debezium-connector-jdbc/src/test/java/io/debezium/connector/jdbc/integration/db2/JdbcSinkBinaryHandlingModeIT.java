/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.db2;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkBinaryHandlingModeTest;
import io.debezium.connector.jdbc.junit.jupiter.Db2SinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;

/**
 * Binary handling mode tests for Db2.
 *
 * @author Minjae Lee
 */
@Tag("all")
@Tag("it")
@Tag("it-db2")
@ExtendWith(Db2SinkDatabaseContextProvider.class)
public class JdbcSinkBinaryHandlingModeIT extends AbstractJdbcSinkBinaryHandlingModeTest {

    public JdbcSinkBinaryHandlingModeIT(Sink sink) {
        super(sink);
    }

    @Override
    protected String characterColumnType() {
        return "varchar(64)";
    }

    @Override
    protected String binaryColumnType() {
        return "varbinary(16)";
    }

    @Override
    protected String largeCharacterColumnType() {
        return "clob(1m)";
    }
}
