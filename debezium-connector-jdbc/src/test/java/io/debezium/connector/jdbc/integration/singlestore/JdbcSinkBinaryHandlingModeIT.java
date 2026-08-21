/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.singlestore;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkBinaryHandlingModeTest;
import io.debezium.connector.jdbc.junit.jupiter.SingleStoreSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;

/**
 * Binary handling mode tests for SingleStore.
 *
 * @author Minjae Lee
 */
@Tag("all")
@Tag("it")
@Tag("it-singlestore")
@ExtendWith(SingleStoreSinkDatabaseContextProvider.class)
public class JdbcSinkBinaryHandlingModeIT extends AbstractJdbcSinkBinaryHandlingModeTest {

    public JdbcSinkBinaryHandlingModeIT(Sink sink) {
        super(sink);
    }

    @Override
    protected String characterColumnType() {
        return "text";
    }

    @Override
    protected String binaryColumnType() {
        return "varbinary(16)";
    }

    @Override
    protected String largeCharacterColumnType() {
        return "longtext";
    }

    @Override
    protected boolean supportsSchemaEvolution() {
        // Upstream has no schema evolution IT coverage for this dialect
        return false;
    }
}
