/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import io.debezium.annotation.Immutable;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.Selectors;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.ColumnNameFilterFactory;
import io.debezium.util.Collect;

/**
 * A utility that is contains various filters for acceptable database names, {@link TableId}s, and columns.
 *
 * @author Randall Hauch
 */
@Immutable
public class Filters {

    protected static final Set<String> BUILT_IN_DB_NAMES = Collect.unmodifiableSet("mysql", "performance_schema", "sys", "information_schema");

    /**
     * A list of tables that are always ignored. Useful for ignoring "phantom"
     * tables occasionally exposed by services such as Amazon RDS Aurora. See
     * DBZ-1939.
     */
    private static final Set<String> IGNORED_TABLE_NAMES = Collect.unmodifiableSet(
            "mysql.rds_configuration",
            "mysql.rds_global_status_history",
            "mysql.rds_global_status_history_old",
            "mysql.rds_history",
            "mysql.rds_replication_status",
            "mysql.rds_sysinfo");

    public static boolean isBuiltInDatabase(String databaseName) {
        if (databaseName == null) {
            return false;
        }
        return BUILT_IN_DB_NAMES.contains(databaseName.toLowerCase());
    }

    private static boolean isBuiltInTable(TableId id) {
        return isBuiltInDatabase(id.catalog());
    }

    private static boolean isIgnoredTable(TableId id) {
        return IGNORED_TABLE_NAMES.contains(id.catalog() + "." + id.table());
    }

    private final Predicate<String> dbFilter;
    private final Predicate<TableId> tableFilter;
    private final Predicate<String> isBuiltInDb;
    private final Predicate<TableId> isBuiltInTable;
    private final ColumnNameFilter columnFilter;
    private final Predicate<TableId> isIgnoredTable;

    private Filters(Predicate<String> dbFilter,
                    Predicate<TableId> tableFilter,
                    Predicate<String> isBuiltInDb,
                    Predicate<TableId> isBuiltInTable,
                    Predicate<TableId> isIgnoredTable,
                    ColumnNameFilter columnFilter) {
        this.dbFilter = dbFilter;
        this.tableFilter = tableFilter;
        this.isBuiltInDb = isBuiltInDb;
        this.isBuiltInTable = isBuiltInTable;
        this.columnFilter = columnFilter;
        this.isIgnoredTable = isIgnoredTable;
    }

    public Predicate<String> databaseFilter() {
        return dbFilter;
    }

    public Predicate<TableId> tableFilter() {
        return tableFilter;
    }

    public Predicate<TableId> ignoredTableFilter() {
        return isIgnoredTable;
    }

    public ColumnNameFilter columnFilter() {
        return columnFilter;
    }

    public static class Builder {

        private Predicate<String> dbFilter;
        private Predicate<TableId> tableFilter;
        private Predicate<String> isBuiltInDb = Filters::isBuiltInDatabase;
        private Predicate<TableId> isBuiltInTable = Filters::isBuiltInTable;
        private Predicate<TableId> isIgnoredTable = Filters::isIgnoredTable;
        private ColumnNameFilter columnFilter;
        private final Configuration config;

        /**
         * Create a Builder for a filter.
         * Set the initial filter data to match the filter data in the given configuration.
         * @param config the configuration of the connector.
         */
        public Builder(Configuration config) {
            this.config = config;
            setFiltersFromStrings(
                    config.getFallbackStringProperty(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, MySqlConnectorConfig.DATABASE_WHITELIST),
                    config.getFallbackStringProperty(MySqlConnectorConfig.DATABASE_EXCLUDE_LIST, MySqlConnectorConfig.DATABASE_BLACKLIST),
                    config.getFallbackStringProperty(MySqlConnectorConfig.TABLE_INCLUDE_LIST, MySqlConnectorConfig.TABLE_WHITELIST),
                    config.getFallbackStringProperty(MySqlConnectorConfig.TABLE_EXCLUDE_LIST, MySqlConnectorConfig.TABLE_BLACKLIST));

            String includeColumnsFilter = config.getString(MySqlConnectorConfig.COLUMN_INCLUDE_LIST);

            if (includeColumnsFilter != null) {
                this.columnFilter = ColumnNameFilterFactory.createIncludeListFilter(includeColumnsFilter, ColumnFilterMode.CATALOG);
            }
            else {
                String excludeColumnsFilter = config.getFallbackStringProperty(MySqlConnectorConfig.COLUMN_EXCLUDE_LIST, MySqlConnectorConfig.COLUMN_BLACKLIST);
                this.columnFilter = ColumnNameFilterFactory.createExcludeListFilter(excludeColumnsFilter, ColumnFilterMode.CATALOG);
            }
        }

        /**
         * Completely reset the filter to match the filter info in the given offsets.
         * This will completely reset the filters to those passed in.
         * @param offsets The offsets to set the filter info to.
         * @return this
         */
        public Builder setFiltersFromOffsets(Map<String, ?> offsets) {
            String dbIncludeList = (String) offsets.get(SourceInfo.DATABASE_INCLUDE_LIST_KEY);
            if (null == dbIncludeList) {
                dbIncludeList = (String) offsets.get(SourceInfo.DATABASE_WHITELIST_KEY);
            }
            String dbExcludeList = (String) offsets.get(SourceInfo.DATABASE_EXCLUDE_LIST_KEY);
            if (null == dbExcludeList) {
                dbExcludeList = (String) offsets.get(SourceInfo.DATABASE_BLACKLIST_KEY);
            }
            String tableIncludeList = (String) offsets.get(SourceInfo.TABLE_INCLUDE_LIST_KEY);
            if (null == tableIncludeList) {
                tableIncludeList = (String) offsets.get(SourceInfo.TABLE_WHITELIST_KEY);
            }
            String tableExcludeList = (String) offsets.get(SourceInfo.TABLE_EXCLUDE_LIST_KEY);
            if (null == tableExcludeList) {
                tableExcludeList = (String) offsets.get(SourceInfo.TABLE_BLACKLIST_KEY);
            }
            setFiltersFromStrings(dbIncludeList, dbExcludeList, tableIncludeList, tableExcludeList);
            return this;
        }

        private void setFiltersFromStrings(String dbIncludeList,
                                           String dbExcludeList,
                                           String tableIncludeList,
                                           String tableExcludeList) {
            Predicate<String> dbFilter = Selectors.databaseSelector()
                    .includeDatabases(dbIncludeList)
                    .excludeDatabases(dbExcludeList)
                    .build();

            // Define the filter using the include and exclude lists for tables and database names ...
            Predicate<TableId> tableFilter = Selectors.tableSelector()
                    .includeDatabases(dbIncludeList)
                    .excludeDatabases(dbExcludeList)
                    .includeTables(tableIncludeList)
                    .excludeTables(tableExcludeList)
                    .build();

            // Ignore built-in databases and tables ...
            if (config.getBoolean(MySqlConnectorConfig.TABLES_IGNORE_BUILTIN)) {
                this.tableFilter = tableFilter.and(isBuiltInTable.negate());
                this.dbFilter = dbFilter.and(isBuiltInDb.negate());
            }
            else {
                this.tableFilter = tableFilter;
                this.dbFilter = dbFilter;
            }

            this.tableFilter = this.tableFilter.and(isIgnoredTable.negate());
        }

        /**
         * Set the filter to match the given other filter.
         * This will completely reset the filters to those passed in.
         * @param filters The other filter
         * @return this
         */
        public Builder setFiltersFromFilters(Filters filters) {
            this.dbFilter = filters.dbFilter;
            this.tableFilter = filters.tableFilter;
            this.isBuiltInDb = filters.isBuiltInDb;
            this.isBuiltInTable = filters.isBuiltInTable;
            this.columnFilter = filters.columnFilter;
            this.isIgnoredTable = filters.isIgnoredTable;
            return this;
        }

        /**
         * Exclude all those tables included by the given filter.
         * @param otherFilter the filter
         * @return this
         */
        public Builder excludeAllTables(Filters otherFilter) {
            excludeDatabases(otherFilter.dbFilter);
            excludeTables(otherFilter.tableFilter);
            return this;
        }

        /**
         * Exclude all the databases that the given predicate tests as true for.
         * @param databases the databases to excluded
         * @return
         */
        public Builder excludeDatabases(Predicate<String> databases) {
            this.dbFilter = this.dbFilter.and(databases.negate());
            return this;
        }

        /**
         * Include all the databases that the given predicate tests as true for.
         * All databases previously included will still be included.
         * @param databases the databases to be included
         * @return
         */
        public Builder includeDatabases(Predicate<String> databases) {
            this.dbFilter = this.dbFilter.or(databases);
            return this;
        }

        /**
         * Exclude all the tables that the given predicate tests as true for.
         * @param tables the tables to be excluded.
         * @return this
         */
        public Builder excludeTables(Predicate<TableId> tables) {
            this.tableFilter = this.tableFilter.and(tables.negate());
            return this;
        }

        /**
         * Include the tables that the given predicate tests as true for.
         * Tables previously included will still be included.
         * @param tables the tables to be included.
         * @return this
         */
        public Builder includeTables(Predicate<TableId> tables) {
            this.tableFilter = this.tableFilter.or(tables);
            return this;
        }

        /**
         * Build the filters.
         * @return the {@link Filters}
         */
        public Filters build() {
            return new Filters(this.dbFilter,
                    this.tableFilter,
                    this.isBuiltInDb,
                    this.isBuiltInTable,
                    this.isIgnoredTable,
                    this.columnFilter);
        }
    }
}
