/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.oracle;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkBinaryHandlingModeTest;
import io.debezium.connector.jdbc.junit.jupiter.OracleSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;

/**
 * Binary handling mode tests for Oracle.
 *
 * @author Minjae Lee
 */
@Tag("all")
@Tag("it")
@Tag("it-oracle")
@ExtendWith(OracleSinkDatabaseContextProvider.class)
public class JdbcSinkBinaryHandlingModeIT extends AbstractJdbcSinkBinaryHandlingModeTest {

    public JdbcSinkBinaryHandlingModeIT(Sink sink) {
        super(sink);
    }

    @Override
    protected String characterColumnType() {
        return "varchar2(64)";
    }

    @Override
    protected String binaryColumnType() {
        return "blob";
    }

    @Override
    protected String largeCharacterColumnType() {
        return "nclob";
    }

    @Override
    protected String nationalCharacterColumnType() {
        return "nvarchar2(64)";
    }
}
