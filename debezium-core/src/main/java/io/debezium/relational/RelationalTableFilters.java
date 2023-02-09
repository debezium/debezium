/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static io.debezium.relational.RelationalDatabaseConnectorConfig.COLUMN_EXCLUDE_LIST;

import java.util.function.Predicate;

import io.debezium.config.Configuration;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.Selectors.TableSelectionPredicateBuilder;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.schema.DataCollectionFilters;

public class RelationalTableFilters implements DataCollectionFilters {

    // Filter that filters tables based only on database/schema/system table filters but not table filters
    // Represents the list of tables whose schema needs to be captured
    private final TableFilter eligibleTableFilter;
    // Filter that filters tables based on table filters
    private final TableFilter tableFilter;
    private final Predicate<String> databaseFilter;
    private final Predicate<String> schemaFilter;
    private final String excludeColumns;

    /**
     * Evaluate whether the table is eligible for schema snapshotting or not.
     * This closely relates to fact whether only captured tables schema should be stored in database
     * history or all tables schema.
     */
    private final TableFilter schemaSnapshotFilter;

    public RelationalTableFilters(Configuration config, TableFilter systemTablesFilter, TableIdToStringMapper tableIdMapper, boolean useCatalogBeforeSchema) {
        // Define the filter that provides the list of tables that could be captured if configured
        final TableSelectionPredicateBuilder eligibleTables = Selectors.tableSelector()
                .includeDatabases(config.getString(RelationalDatabaseConnectorConfig.DATABASE_INCLUDE_LIST))
                .excludeDatabases(config.getString(RelationalDatabaseConnectorConfig.DATABASE_EXCLUDE_LIST))
                .includeSchemas(config.getString(RelationalDatabaseConnectorConfig.SCHEMA_INCLUDE_LIST))
                .excludeSchemas(config.getString(RelationalDatabaseConnectorConfig.SCHEMA_EXCLUDE_LIST));
        final Predicate<TableId> eligibleTablePredicate = eligibleTables.build();

        Predicate<TableId> finalEligibleTablePredicate = config.getBoolean(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN)
                ? eligibleTablePredicate.and(systemTablesFilter::isIncluded)
                : eligibleTablePredicate;

        this.eligibleTableFilter = finalEligibleTablePredicate::test;

        // Define the filter using the include and exclude lists for tables and database names ...
        Predicate<TableId> tablePredicate = eligibleTables
                .includeTables(
                        config.getString(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST),
                        tableIdMapper)
                .excludeTables(
                        config.getString(RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST),
                        tableIdMapper)
                .build();

        Predicate<TableId> finalTablePredicate = config.getBoolean(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN)
                ? tablePredicate.and(systemTablesFilter::isIncluded)
                : tablePredicate;
        String signalDataCollection = config.getString(RelationalDatabaseConnectorConfig.SIGNAL_DATA_COLLECTION);
        if (signalDataCollection != null) {
            TableId signalDataCollectionTableId = TableId.parse(signalDataCollection, useCatalogBeforeSchema);
            if (!finalTablePredicate.test(signalDataCollectionTableId)) {
                final Predicate<TableId> signalDataCollectionPredicate = Selectors.tableSelector()
                        .includeTables(tableIdMapper.toString(signalDataCollectionTableId), tableIdMapper).build();
                finalTablePredicate = finalTablePredicate.or(signalDataCollectionPredicate);
            }
        }
        this.tableFilter = finalTablePredicate::test;

        // Define the database filter using the include and exclude lists for database names ...
        this.databaseFilter = Selectors.databaseSelector()
                .includeDatabases(config.getString(RelationalDatabaseConnectorConfig.DATABASE_INCLUDE_LIST))
                .excludeDatabases(config.getString(RelationalDatabaseConnectorConfig.DATABASE_EXCLUDE_LIST))
                .build();
        this.schemaFilter = Selectors.databaseSelector()
                .includeDatabases(config.getString(RelationalDatabaseConnectorConfig.SCHEMA_INCLUDE_LIST))
                .excludeDatabases(config.getString(RelationalDatabaseConnectorConfig.SCHEMA_EXCLUDE_LIST))
                .build();

        Predicate<TableId> eligibleSchemaPredicate = config.getBoolean(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN)
                ? systemTablesFilter::isIncluded
                : x -> true;

        this.schemaSnapshotFilter = config.getBoolean(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL)
                ? eligibleSchemaPredicate.and(tableFilter::isIncluded)::test
                : eligibleSchemaPredicate::test;

        this.excludeColumns = config.getString(COLUMN_EXCLUDE_LIST);
    }

    @Override
    public TableFilter dataCollectionFilter() {
        return tableFilter;
    }

    public TableFilter eligibleDataCollectionFilter() {
        return eligibleTableFilter;
    }

    public TableFilter eligibleForSchemaDataCollectionFilter() {
        return schemaSnapshotFilter;
    }

    public Predicate<String> databaseFilter() {
        return databaseFilter;
    }

    public Predicate<String> schemaFilter() {
        return schemaFilter;
    }

    public String getExcludeColumns() {
        return excludeColumns;
    }
}
