/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static io.debezium.relational.RelationalDatabaseConnectorConfig.COLUMN_BLACKLIST;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.COLUMN_EXCLUDE_LIST;

import java.util.function.Predicate;

import io.debezium.config.Configuration;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.schema.DataCollectionFilters;

public class RelationalTableFilters implements DataCollectionFilters {

    private final TableFilter tableFilter;
    private final String excludeColumns;

    public RelationalTableFilters(Configuration config, TableFilter systemTablesFilter, TableIdToStringMapper tableIdMapper) {
        // Define the filter using the include and exclude lists for tables and database names ...
        Predicate<TableId> predicate = Selectors.tableSelector()
                .includeSchemas(
                        config.getFallbackStringProperty(
                                RelationalDatabaseConnectorConfig.SCHEMA_INCLUDE_LIST,
                                RelationalDatabaseConnectorConfig.SCHEMA_WHITELIST))
                .excludeSchemas(
                        config.getFallbackStringProperty(
                                RelationalDatabaseConnectorConfig.SCHEMA_EXCLUDE_LIST,
                                RelationalDatabaseConnectorConfig.SCHEMA_BLACKLIST))
                .includeTables(
                        config.getFallbackStringProperty(
                                RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST,
                                RelationalDatabaseConnectorConfig.TABLE_WHITELIST),
                        tableIdMapper)
                .excludeTables(
                        config.getFallbackStringProperty(
                                RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST,
                                RelationalDatabaseConnectorConfig.TABLE_BLACKLIST),
                        tableIdMapper)
                .build();

        Predicate<TableId> finalPredicate = config.getBoolean(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN)
                ? predicate.and(systemTablesFilter::isIncluded)
                : predicate;

        this.tableFilter = finalPredicate::test;
        this.excludeColumns = config.getFallbackStringProperty(COLUMN_EXCLUDE_LIST, COLUMN_BLACKLIST);
    }

    @Override
    public TableFilter dataCollectionFilter() {
        return tableFilter;
    }

    public String getExcludeColumns() {
        return excludeColumns;
    }
}
