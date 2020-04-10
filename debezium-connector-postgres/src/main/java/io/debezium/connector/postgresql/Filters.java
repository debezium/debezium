/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import io.debezium.annotation.Immutable;
import io.debezium.relational.Selectors;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.ColumnNameFilterFactory;
import io.debezium.relational.Tables.TableFilter;

/**
 * A utility that is contains various filters for acceptable {@link TableId}s and columns.
 *
 * @author Horia Chiorean
 */
@Immutable
public class Filters {

    protected static final List<String> SYSTEM_SCHEMAS = Arrays.asList("pg_catalog", "information_schema");
    protected static final String SYSTEM_SCHEMA_BLACKLIST = String.join(",", SYSTEM_SCHEMAS);
    protected static final Predicate<String> IS_SYSTEM_SCHEMA = SYSTEM_SCHEMAS::contains;
    protected static final String TEMP_TABLE_BLACKLIST = ".*\\.pg_temp.*";

    private final TableFilter tableFilter;
    private final ColumnNameFilter columnFilter;

    /**
     * @param config the configuration; may not be null
     */
    public Filters(PostgresConnectorConfig config) {

        // we always want to exclude PG system schemas as they are never part of logical decoding events
        String schemaBlacklist = config.schemaBlacklist();
        if (schemaBlacklist != null) {
            schemaBlacklist = schemaBlacklist + "," + SYSTEM_SCHEMA_BLACKLIST;
        }
        else {
            schemaBlacklist = SYSTEM_SCHEMA_BLACKLIST;
        }

        String tableBlacklist = config.tableBlacklist();
        if (tableBlacklist != null) {
            tableBlacklist = tableBlacklist + "," + TEMP_TABLE_BLACKLIST;
        }
        else {
            tableBlacklist = TEMP_TABLE_BLACKLIST;
        }

        // Define the filter using the whitelists and blacklists for table names ...
        this.tableFilter = TableFilter.fromPredicate(Selectors.tableSelector()
                .includeTables(config.tableWhitelist())
                .excludeTables(tableBlacklist)
                .includeSchemas(config.schemaWhitelist())
                .excludeSchemas(schemaBlacklist)
                .build());

        String columnWhitelist = config.columnWhitelist();
        if (columnWhitelist != null) {
            this.columnFilter = ColumnNameFilterFactory.createWhitelistFilter(config.columnWhitelist());
        }
        else {
            // Define the filter that excludes blacklisted columns, truncated columns, and masked columns ...
            this.columnFilter = ColumnNameFilterFactory.createBlacklistFilter(config.columnBlacklist());
        }
    }

    protected TableFilter tableFilter() {
        return tableFilter;
    }

    protected ColumnNameFilter columnFilter() {
        return columnFilter;
    }

}
