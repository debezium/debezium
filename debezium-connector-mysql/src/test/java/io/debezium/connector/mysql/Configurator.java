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
public class Configurator {

    private final Configuration.Builder configBuilder = Configuration.create();

    public Configurator with(Field field, String value) {
        configBuilder.with(field, value);
        return this;
    }

    public Configurator with(Field field, boolean value) {
        configBuilder.with(field, value);
        return this;
    }

    public Configurator serverName(String serverName) {
        return with(MySqlConnectorConfig.SERVER_NAME, serverName);
    }

    public Configurator includeDatabases(String regexList) {
        return with(MySqlConnectorConfig.DATABASE_WHITELIST, regexList);
    }

    public Configurator excludeDatabases(String regexList) {
        return with(MySqlConnectorConfig.DATABASE_BLACKLIST, regexList);
    }

    public Configurator includeTables(String regexList) {
        return with(MySqlConnectorConfig.TABLE_WHITELIST, regexList);
    }

    public Configurator excludeTables(String regexList) {
        return with(MySqlConnectorConfig.TABLE_BLACKLIST, regexList);
    }

    public Configurator excludeColumns(String regexList) {
        return with(MySqlConnectorConfig.COLUMN_BLACKLIST, regexList);
    }

    public Configurator truncateColumns(int length, String fullyQualifiedTableNames) {
        return with(MySqlConnectorConfig.TRUNCATE_COLUMN(length), fullyQualifiedTableNames);
    }

    public Configurator maskColumns(int length, String fullyQualifiedTableNames) {
        return with(MySqlConnectorConfig.MASK_COLUMN(length), fullyQualifiedTableNames);
    }

    public Configurator excludeBuiltInTables() {
        return with(MySqlConnectorConfig.TABLES_IGNORE_BUILTIN, true);
    }

    public Configurator includeBuiltInTables() {
        return with(MySqlConnectorConfig.TABLES_IGNORE_BUILTIN, false);
    }

    public Configurator storeDatabaseHistoryInFile(Path path) {
        with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class.getName());
        with(FileDatabaseHistory.FILE_PATH,path.toAbsolutePath().toString());
        return this;
    }

    public Filters createFilters() {
        return new Filters(configBuilder.build());
    }

    /**
     * For tests use only
     */
    public MySqlSchema createSchemas() {
        Configuration config = configBuilder.build();
        String serverName = config.getString(MySqlConnectorConfig.SERVER_NAME);

        return new MySqlSchema(config, serverName, null, false,
                TopicSelector.defaultSelector(serverName, "__debezium-heartbeat"));
    }
}
