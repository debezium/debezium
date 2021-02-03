/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import java.nio.file.Path;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Testing;

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
        if (Math.random() >= 0.5) {
            Testing.debug("Using \"" + MySqlConnectorConfig.DATABASE_WHITELIST.name() + "\" config property");
            return with(MySqlConnectorConfig.DATABASE_WHITELIST, regexList);
        }
        Testing.debug("Using \"" + MySqlConnectorConfig.DATABASE_INCLUDE_LIST.name() + "\" config property");
        return with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, regexList);
    }

    /* package local */ Configurator excludeDatabases(String regexList) {
        if (Math.random() >= 0.5) {
            Testing.debug("Using \"" + MySqlConnectorConfig.DATABASE_BLACKLIST.name() + "\" config property");
            return with(MySqlConnectorConfig.DATABASE_BLACKLIST, regexList);
        }
        Testing.debug("Using \"" + MySqlConnectorConfig.DATABASE_EXCLUDE_LIST.name() + "\" config property");
        return with(MySqlConnectorConfig.DATABASE_EXCLUDE_LIST, regexList);
    }

    /* package local */ Configurator includeTables(String regexList) {
        if (Math.random() >= 0.5) {
            Testing.debug("Using \"" + MySqlConnectorConfig.TABLE_WHITELIST.name() + "\" config property");
            return with(MySqlConnectorConfig.TABLE_WHITELIST, regexList);
        }
        Testing.debug("Using \"" + MySqlConnectorConfig.TABLE_INCLUDE_LIST.name() + "\" config property");
        return with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, regexList);
    }

    /* package local */ Configurator excludeTables(String regexList) {
        if (Math.random() >= 0.5) {
            Testing.debug("Using \"" + MySqlConnectorConfig.TABLE_BLACKLIST.name() + "\" config property");
            return with(MySqlConnectorConfig.TABLE_BLACKLIST, regexList);
        }
        Testing.debug("Using \"" + MySqlConnectorConfig.TABLE_EXCLUDE_LIST.name() + "\" config property");
        return with(MySqlConnectorConfig.TABLE_EXCLUDE_LIST, regexList);
    }

    /* package local */ Configurator includeColumns(String regexList) {
        return with(MySqlConnectorConfig.COLUMN_INCLUDE_LIST, regexList);
    }

    /* package local */ Configurator excludeColumns(String regexList) {
        if (Math.random() >= 0.5) {
            Testing.debug("Using \"" + MySqlConnectorConfig.COLUMN_BLACKLIST.name() + "\" config property");
            return with(MySqlConnectorConfig.COLUMN_BLACKLIST, regexList);
        }
        Testing.debug("Using \"" + MySqlConnectorConfig.COLUMN_EXCLUDE_LIST.name() + "\" config property");
        return with(MySqlConnectorConfig.COLUMN_EXCLUDE_LIST, regexList);
    }

    /* package local */ Configurator truncateColumns(int length, String fullyQualifiedTableNames) {
        if (length <= 0) {
            throw new IllegalArgumentException("The truncation length must be positive");
        }
        return with(Field.create("column.truncate.to." + length + ".chars")
                .withValidation(Field::isInteger)
                .withDescription("A comma-separated list of regular expressions matching fully-qualified names of columns that should "
                        + "be truncated to " + length + " characters."),
                fullyQualifiedTableNames);
    }

    /* package local */ Configurator maskColumns(int length, String fullyQualifiedTableNames) {
        if (length <= 0) {
            throw new IllegalArgumentException("The mask length must be positive");
        }
        return with(Field.create("column.mask.with." + length + ".chars")
                .withValidation(Field::isInteger)
                .withDescription("A comma-separated list of regular expressions matching fully-qualified names of columns that should "
                        + "be masked with " + length + " asterisk ('*') characters."),
                fullyQualifiedTableNames);
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
