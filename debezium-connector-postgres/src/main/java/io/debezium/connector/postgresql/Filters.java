/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.debezium.annotation.Immutable;
import io.debezium.relational.ColumnId;
import io.debezium.relational.Selectors;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

/**
 * A utility that is contains various filters for acceptable {@link TableId}s and columns.
 * 
 * @author Horia Chiorean
 */
@Immutable
public class Filters {
    
    protected static final List<String> SYSTEM_SCHEMAS = Arrays.asList("pg_catalog", "information_schema");
    protected static final String SYSTEM_SCHEMA_BLACKLIST = SYSTEM_SCHEMAS.stream().collect(Collectors.joining(","));
    protected static final Predicate<String> IS_SYSTEM_SCHEMA = SYSTEM_SCHEMAS::contains;
    protected static final String TEMP_TABLE_BLACKLIST = ".*\\.pg_temp.*"; 
    
    private final Predicate<TableId> tableFilter;
    private final Predicate<ColumnId> columnFilter;

    /**
     * @param config the configuration; may not be null
     */
    public Filters(PostgresConnectorConfig config) {
      
        // we always want to exclude PG system schemas as they are never part of logical decoding events
        String schemaBlacklist = config.schemaBlacklist();
        if (schemaBlacklist != null) {
            schemaBlacklist = schemaBlacklist + "," + SYSTEM_SCHEMA_BLACKLIST;
        } else {
            schemaBlacklist = SYSTEM_SCHEMA_BLACKLIST;
        }
    
        String tableBlacklist = config.tableBlacklist();
        if (tableBlacklist != null) {
            tableBlacklist = tableBlacklist + "," + TEMP_TABLE_BLACKLIST;                
        } else {
            tableBlacklist = TEMP_TABLE_BLACKLIST;
        }
        
        // Define the filter using the whitelists and blacklists for table names ...
        this.tableFilter = Selectors.tableSelector()
                                    .includeTables(config.tableWhitelist())
                                    .excludeTables(tableBlacklist)
                                    .includeSchemas(config.schemaWhitelist())
                                    .excludeSchemas(schemaBlacklist)
                                    .build();

        
        // Define the filter that excludes blacklisted columns, truncated columns, and masked columns ...
        this.columnFilter = Selectors.excludeColumns(config.columnBlacklist());
    }
    
    protected Predicate<TableId> tableFilter() {
        return tableFilter;
    }
    
    protected Predicate<ColumnId> columnFilter() {
        return columnFilter;
    }   
    
    protected Tables.TableNameFilter tableNameFilter() {
        return Tables.filterFor(tableFilter);
    } 
}
