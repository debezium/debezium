/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.debezium.annotation.Immutable;
import io.debezium.config.Configuration;
import io.debezium.relational.ColumnId;
import io.debezium.relational.Selectors;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.TableNameFilter;
import io.debezium.relational.mapping.ColumnMappers;
import io.debezium.util.Collect;

/**
 * A utility that is contains various filters for acceptable database names, {@link TableId}s, and columns.
 * 
 * @author Randall Hauch
 */
@Immutable
public class Filters {

    protected static final Set<String> BUILT_IN_DB_NAMES = Collect.unmodifiableSet("mysql", "performance_schema","sys", "information_schema");

    protected static boolean isBuiltInDatabase(String databaseName) {
        return BUILT_IN_DB_NAMES.contains(databaseName.toLowerCase());
    }

    protected static boolean isBuiltInTable(TableId id ) {
        return isBuiltInDatabase(id.catalog());
    }

    protected static boolean isNotBuiltInDatabase(String databaseName) {
        return !isBuiltInDatabase(databaseName);
    }

    protected static boolean isNotBuiltInTable(TableId id ) {
        return !isBuiltInTable(id);
    }

    protected static List<TableId> withoutBuiltIns(Collection<TableId> tableIds) {
        return tableIds.stream().filter(Filters::isNotBuiltInTable).collect(Collectors.toList());
    }

    protected static List<String> withoutBuiltInDatabases(Collection<String> dbNames) {
        return dbNames.stream().filter(Filters::isNotBuiltInDatabase).collect(Collectors.toList());
    }

    private final Predicate<String> dbFilter;
    private final Predicate<TableId> tableFilter;
    private final Predicate<String> isBuiltInDb;
    private final Predicate<TableId> isBuiltInTable;
    private final Predicate<ColumnId> columnFilter;
    private final ColumnMappers columnMappers;

    /**
     * @param config the configuration; may not be null
     */
    public Filters(Configuration config) {
        this.isBuiltInDb = Filters::isBuiltInDatabase;
        this.isBuiltInTable = Filters::isBuiltInTable;

        // Define the filter used for database names ...
        Predicate<String> dbFilter = Selectors.databaseSelector()
                                              .includeDatabases(config.getString(MySqlConnectorConfig.DATABASE_WHITELIST))
                                              .excludeDatabases(config.getString(MySqlConnectorConfig.DATABASE_BLACKLIST))
                                              .build();

        // Define the filter using the whitelists and blacklists for tables and database names ...
        Predicate<TableId> tableFilter = Selectors.tableSelector()
                                                  .includeDatabases(config.getString(MySqlConnectorConfig.DATABASE_WHITELIST))
                                                  .excludeDatabases(config.getString(MySqlConnectorConfig.DATABASE_BLACKLIST))
                                                  .includeTables(config.getString(MySqlConnectorConfig.TABLE_WHITELIST))
                                                  .excludeTables(config.getString(MySqlConnectorConfig.TABLE_BLACKLIST))
                                                  .build();

        // Ignore built-in databases and tables ...
        if (config.getBoolean(MySqlConnectorConfig.TABLES_IGNORE_BUILTIN)) {
            this.tableFilter = tableFilter.and(isBuiltInTable.negate());
            this.dbFilter = dbFilter.and(isBuiltInDb.negate());
        } else {
            this.tableFilter = tableFilter;
            this.dbFilter = dbFilter;
        }

        // Define the filter that excludes blacklisted columns, truncated columns, and masked columns ...
        this.columnFilter = Selectors.excludeColumns(config.getString(MySqlConnectorConfig.COLUMN_BLACKLIST));

        // Define the truncated, masked, and mapped columns ...
        ColumnMappers.Builder columnMapperBuilder = ColumnMappers.create();
        config.forEachMatchingFieldNameWithInteger("column\\.truncate\\.to\\.(\\d+)\\.chars", columnMapperBuilder::truncateStrings);
        config.forEachMatchingFieldNameWithInteger("column\\.mask\\.with\\.(\\d+)\\.chars", columnMapperBuilder::maskStrings);
        this.columnMappers = columnMapperBuilder.build();
    }

    public Predicate<String> databaseFilter() {
        return dbFilter;
    }

    public Predicate<TableId> tableInDatabaseFilter() {
        return tableId->{
            return dbFilter.test(tableId.catalog());
        };
    }
    
    public Predicate<TableId> tableFilter() {
        return tableFilter;
    }
    
    public TableNameFilter tableNameFilter() {
        return Tables.filterFor(tableFilter);
    }

    public Predicate<TableId> builtInTableFilter() {
        return isBuiltInTable;
    }

    public Predicate<String> builtInDatabaseFilter() {
        return isBuiltInDb;
    }

    public Predicate<ColumnId> columnFilter() {
        return columnFilter;
    }

    public ColumnMappers columnMappers() {
        return columnMappers;
    }

}
