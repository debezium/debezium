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
import io.debezium.relational.Selectors.TableSelectionPredicateBuilder;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.schema.DataCollectionFilters;

public class RelationalTableFilters implements DataCollectionFilters {

    // Filter that filters tables based only on datbase/schema/system table filters but not table filters
    // Represents the list of tables whose schema needs to be captured
    private final TableFilter eligibleTableFilter;
    // Filter that filters tables based on table filters
    private final TableFilter tableFilter;
    private final Predicate<String> databaseFilter;
    private final String excludeColumns;

    public RelationalTableFilters(Configuration config, TableFilter systemTablesFilter, TableIdToStringMapper tableIdMapper) {
        // Define the filter that provides the list of tables that could be captured if configured
        final TableSelectionPredicateBuilder eligibleTables = Selectors.tableSelector()
                .includeDatabases(
                        config.getFallbackStringProperty(
                                RelationalDatabaseConnectorConfig.DATABASE_INCLUDE_LIST,
                                RelationalDatabaseConnectorConfig.DATABASE_WHITELIST))
                .excludeDatabases(
                        config.getFallbackStringProperty(
                                RelationalDatabaseConnectorConfig.DATABASE_EXCLUDE_LIST,
                                RelationalDatabaseConnectorConfig.DATABASE_BLACKLIST))
                .includeSchemas(
                        config.getFallbackStringProperty(
                                RelationalDatabaseConnectorConfig.SCHEMA_INCLUDE_LIST,
                                RelationalDatabaseConnectorConfig.SCHEMA_WHITELIST))
                .excludeSchemas(
                        config.getFallbackStringProperty(
                                RelationalDatabaseConnectorConfig.SCHEMA_EXCLUDE_LIST,
                                RelationalDatabaseConnectorConfig.SCHEMA_BLACKLIST));
        final Predicate<TableId> eligibleTablePredicate = eligibleTables.build();

        Predicate<TableId> finalEligibleTablePredicate = config.getBoolean(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN)
                ? eligibleTablePredicate.and(systemTablesFilter::isIncluded)
                : eligibleTablePredicate;

        this.eligibleTableFilter = finalEligibleTablePredicate::test;

        // Define the filter using the include and exclude lists for tables and database names ...
        Predicate<TableId> tablePredicate = eligibleTables
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

        Predicate<TableId> finalTablePredicate = config.getBoolean(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN)
                ? tablePredicate.and(systemTablesFilter::isIncluded)
                : tablePredicate;

        this.tableFilter = finalTablePredicate::test;

        // Define the database filter using the include and exclude lists for database names ...
        this.databaseFilter = Selectors.databaseSelector()
                .includeDatabases(
                        config.getFallbackStringProperty(
                                RelationalDatabaseConnectorConfig.DATABASE_INCLUDE_LIST,
                                RelationalDatabaseConnectorConfig.DATABASE_WHITELIST))
                .excludeDatabases(
                        config.getFallbackStringProperty(
                                RelationalDatabaseConnectorConfig.DATABASE_EXCLUDE_LIST,
                                RelationalDatabaseConnectorConfig.DATABASE_BLACKLIST))
                .build();

        this.excludeColumns = config.getFallbackStringProperty(COLUMN_EXCLUDE_LIST, COLUMN_BLACKLIST);
    }

    @Override
    public TableFilter dataCollectionFilter() {
        return tableFilter;
    }

    public TableFilter eligibleDataCollectionFilter() {
        return eligibleTableFilter;
    }

    public Predicate<String> databaseFilter() {
        return databaseFilter;
    }

    public String getExcludeColumns() {
        return excludeColumns;
    }
}
