/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.nio.file.Path;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.history.FileDatabaseHistory;

/**
 * A helper for easily building connector configurations for testing.
 *
 * @author Randall Hauch
 */
/* package local */ class Configurator {

    private final Configuration.Builder configBuilder = Configuration.create();

    /* package local */ Configurator with(Field field, String value) {
        configBuilder.with(field, value);
        return this;
    }

    /* package local */ Configurator with(Field field, boolean value) {
        configBuilder.with(field, value);
        return this;
    }

    /* package local */ Configurator serverName(String serverName) {
        return with(MySqlConnectorConfig.SERVER_NAME, serverName);
    }

    /* package local */ Configurator includeDatabases(String regexList) {
        return with(MySqlConnectorConfig.DATABASE_WHITELIST, regexList);
    }

    /* package local */ Configurator excludeDatabases(String regexList) {
        return with(MySqlConnectorConfig.DATABASE_BLACKLIST, regexList);
    }

    /* package local */ Configurator includeTables(String regexList) {
        return with(MySqlConnectorConfig.TABLE_WHITELIST, regexList);
    }

    /* package local */ Configurator excludeTables(String regexList) {
        return with(MySqlConnectorConfig.TABLE_BLACKLIST, regexList);
    }

    /* package local */ Configurator excludeColumns(String regexList) {
        return with(MySqlConnectorConfig.COLUMN_BLACKLIST, regexList);
    }

    /* package local */ Configurator truncateColumns(int length, String fullyQualifiedTableNames) {
        return with(MySqlConnectorConfig.TRUNCATE_COLUMN(length), fullyQualifiedTableNames);
    }

    /* package local */ Configurator maskColumns(int length, String fullyQualifiedTableNames) {
        return with(MySqlConnectorConfig.MASK_COLUMN(length), fullyQualifiedTableNames);
    }

    /* package local */ Configurator excludeBuiltInTables() {
        return with(MySqlConnectorConfig.TABLES_IGNORE_BUILTIN, true);
    }

    /* package local */ Configurator includeBuiltInTables() {
        return with(MySqlConnectorConfig.TABLES_IGNORE_BUILTIN, false);
    }

    /* package local */ Configurator storeDatabaseHistoryInFile(Path path) {
        with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class.getName());
        with(FileDatabaseHistory.FILE_PATH, path.toAbsolutePath().toString());
        return this;
    }

    /* package local */ Filters createFilters() {
        return new Filters.Builder(configBuilder.build()).build();
    }

    /**
     * For tests use only
     */
    /* package local */ MySqlSchema createSchemas() {
        return createSchemasWithFilter(createFilters());
    }

    /* package local */ MySqlSchema createSchemasWithFilter(Filters filters) {
        Configuration config = configBuilder.build();
        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);

        return new MySqlSchema(connectorConfig,
                null,
                false,
                MySqlTopicSelector.defaultSelector(connectorConfig.getLogicalName(), "__debezium-heartbeat"),
                filters);
    }
}
