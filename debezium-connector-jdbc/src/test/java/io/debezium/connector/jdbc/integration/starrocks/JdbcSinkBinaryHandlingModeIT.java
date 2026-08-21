/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.starrocks;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkBinaryHandlingModeTest;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.StarRocksSinkDatabaseContextProvider;

/**
 * Binary handling mode tests for StarRocks.
 *
 * @author Minjae Lee
 */
@Tag("all")
@Tag("it")
@Tag("it-starrocks")
@ExtendWith(StarRocksSinkDatabaseContextProvider.class)
public class JdbcSinkBinaryHandlingModeIT extends AbstractJdbcSinkBinaryHandlingModeTest {

    public JdbcSinkBinaryHandlingModeIT(Sink sink) {
        super(sink);
    }

    @Override
    protected String characterColumnType() {
        return "string";
    }

    @Override
    protected String binaryColumnType() {
        return "varbinary(16)";
    }

    @Override
    protected String singleDataColumnTableDdl(String tableName, String dataColumnType) {
        return String.format("CREATE TABLE %s (id tinyint NOT NULL, data %s NULL) PRIMARY KEY(id) DISTRIBUTED BY HASH(id)",
                tableName, dataColumnType);
    }

    @Override
    protected String selectorColumnsTableDdl(String tableName) {
        return String.format(
                "CREATE TABLE %s (id tinyint NOT NULL, data_hex %s NULL, data_b64 %s NULL, data_raw %s NULL) PRIMARY KEY(id) DISTRIBUTED BY HASH(id)",
                tableName, characterColumnType(), characterColumnType(), binaryColumnType());
    }
}
