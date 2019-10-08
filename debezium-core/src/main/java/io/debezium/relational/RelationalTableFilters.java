/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.function.Predicate;

import io.debezium.config.Configuration;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.schema.DataCollectionFilters;

public class RelationalTableFilters implements DataCollectionFilters {

    private final TableFilter tableFilter;

    public RelationalTableFilters(Configuration config, TableFilter systemTablesFilter, TableIdToStringMapper tableIdMapper) {
        // Define the filter using the whitelists and blacklists for tables and database names ...
        Predicate<TableId> predicate = Selectors.tableSelector()
                // .includeDatabases(config.getString(RelationalDatabaseConnectorConfig.DATABASE_WHITELIST))
                // .excludeDatabases(config.getString(RelationalDatabaseConnectorConfig.DATABASE_BLACKLIST))
                .includeSchemas(config.getString(RelationalDatabaseConnectorConfig.SCHEMA_WHITELIST))
                .excludeSchemas(config.getString(RelationalDatabaseConnectorConfig.SCHEMA_BLACKLIST))
                .includeTables(config.getString(RelationalDatabaseConnectorConfig.TABLE_WHITELIST), tableIdMapper)
                .excludeTables(config.getString(RelationalDatabaseConnectorConfig.TABLE_BLACKLIST), tableIdMapper)
                .build();

        Predicate<TableId> finalPredicate = config.getBoolean(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN)
                ? predicate.and(systemTablesFilter::isIncluded)
                : predicate;

        this.tableFilter = t -> finalPredicate.test(t);
    }

    @Override
    public TableFilter dataCollectionFilter() {
        return tableFilter;
    }
}
