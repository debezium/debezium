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
import io.debezium.relational.Tables.TableFilter;

/**
 * A utility that is contains various filters for acceptable {@link TableId}s and columns.
 *
 * @author Horia Chiorean
 */
@Immutable
public class Filters {

    protected static final List<String> SYSTEM_SCHEMAS = Arrays.asList("pg_catalog", "information_schema");
    protected static final String SYSTEM_SCHEMA_EXCLUDE_LIST = String.join(",", SYSTEM_SCHEMAS);
    protected static final Predicate<String> IS_SYSTEM_SCHEMA = SYSTEM_SCHEMAS::contains;
    protected static final String TEMP_TABLE_EXCLUDE_LIST = ".*\\.pg_temp.*";

    private final TableFilter tableFilter;

    /**
     * @param config the configuration; may not be null
     */
    public Filters(PostgresConnectorConfig config) {

        // we always want to exclude PG system schemas as they are never part of logical decoding events
        String schemaExcludeList = config.schemaExcludeList();
        if (schemaExcludeList != null) {
            schemaExcludeList = schemaExcludeList + "," + SYSTEM_SCHEMA_EXCLUDE_LIST;
        }
        else {
            schemaExcludeList = SYSTEM_SCHEMA_EXCLUDE_LIST;
        }

        String tableExcludeList = config.tableExcludeList();
        if (tableExcludeList != null) {
            tableExcludeList = tableExcludeList + "," + TEMP_TABLE_EXCLUDE_LIST;
        }
        else {
            tableExcludeList = TEMP_TABLE_EXCLUDE_LIST;
        }

        // Define the filter using the include/exclude lists for table names ...
        this.tableFilter = TableFilter.fromPredicate(Selectors.tableSelector()
                .includeTables(config.tableIncludeList())
                .excludeTables(tableExcludeList)
                .includeSchemas(config.schemaIncludeList())
                .excludeSchemas(schemaExcludeList)
                .build());
    }

    protected TableFilter tableFilter() {
        return tableFilter;
    }
}
