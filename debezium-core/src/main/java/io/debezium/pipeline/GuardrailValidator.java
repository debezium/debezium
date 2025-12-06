/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.HistorizedDatabaseSchema;

/**
 * Validator for guardrail limits across different connector types.
 * Provides common logic for validating that the number of captured tables/collections
 * does not exceed configured guardrail limits.
 *
 * @author Debezium Community
 */
public class GuardrailValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(GuardrailValidator.class);

    private final CommonConnectorConfig connectorConfig;
    private final RelationalTableFilters tableFilters;
    private final Boolean storeOnlyCapturedTables;
    private final Boolean storeOnlyCapturedDatabases;

    /**
     * Constructor for collection-based connectors (e.g., MongoDB).
     * <p>
     * Use this constructor for connectors that don't use relational table filters.
     *
     * @param connectorConfig The connector configuration
     */
    public GuardrailValidator(CommonConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
        this.tableFilters = null;
        this.storeOnlyCapturedTables = null;
        this.storeOnlyCapturedDatabases = null;
    }

    /**
     * Constructor for relational database connectors.
     *
     * @param connectorConfig The connector configuration
     * @param schema The database schema
     */
    public GuardrailValidator(CommonConnectorConfig connectorConfig, DatabaseSchema<?> schema) {
        this.connectorConfig = connectorConfig;

        // Extract table filters from the config if it's a relational connector config
        if (connectorConfig instanceof RelationalDatabaseConnectorConfig relationalConfig) {
            this.tableFilters = relationalConfig.getTableFilters();
        }
        else {
            this.tableFilters = null;
        }

        // Extract schema history settings from the schema if it's historized
        if (schema instanceof HistorizedDatabaseSchema<?> historizedSchema) {
            this.storeOnlyCapturedTables = historizedSchema.storeOnlyCapturedTables();
            this.storeOnlyCapturedDatabases = historizedSchema.storeOnlyCapturedDatabases();
        }
        else {
            // Non-historized schemas (like PostgreSQL) default to null
            this.storeOnlyCapturedTables = null;
            this.storeOnlyCapturedDatabases = null;
        }
    }

    /**
     * Validates guardrail limits for table-based connectors.
     *
     * @param allTableIds The set of all table IDs to validate
     */
    public void validate(Set<TableId> allTableIds) {
        List<String> tableNames;
        // For non-historized connectors (PostgreSQL), storeOnlyCapturedTables will be null
        // For historized connectors, it will be true or false
        // null or true means we validate only captured tables
        // false means we validate all tables in the schema
        boolean isValidatingAllTables = Boolean.FALSE.equals(storeOnlyCapturedTables);

        if (isValidatingAllTables) {
            // Schema-history based connectors with storeOnlyCapturedTables=false
            tableNames = allTableIds.stream()
                    .filter(tableId -> tableFilters.eligibleForSchemaDataCollectionFilter().isIncluded(tableId))
                    .map(TableId::toString)
                    .collect(Collectors.toList());
            LOGGER.info("Validating guardrail limits against {} tables present in {}",
                    tableNames.size(),
                    Boolean.TRUE.equals(storeOnlyCapturedDatabases) ? "the captured databases" : "all databases");
        }
        else {
            // Non-historized connectors (null) or historized with storeOnlyCapturedTables=true
            tableNames = allTableIds.stream()
                    .filter(tableId -> tableFilters.dataCollectionFilter().isIncluded(tableId))
                    .map(TableId::toString)
                    .collect(Collectors.toList());
            LOGGER.info("Validating guardrail limits against {} captured tables", tableNames.size());
        }

        connectorConfig.validateGuardrailLimits(tableNames, isValidatingAllTables);
    }

    /**
     * Validates guardrail limits for collection-based connectors (e.g., MongoDB).
     * This overload works with collection names instead of TableId objects.
     *
     * @param collectionNames The list of collection names to validate
     */
    public void validate(Collection<String> collectionNames) {
        LOGGER.info("Validating guardrail limits against {} captured collections", collectionNames.size());
        connectorConfig.validateGuardrailLimits(collectionNames);
    }
}
