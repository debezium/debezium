/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;

/**
 * Validator for guardrail limits across different connector types.
 * Provides common logic for validating that the number of captured tables/collections
 * does not exceed configured guardrail limits.
 *
 * @author Debezium Community
 */
public class GuardrailValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(GuardrailValidator.class);

    /**
     * Validates table guardrail limits for schema-history based connectors that support
     * the storeOnlyCapturedTables configuration.
     * <p>
     * This method handles the common logic for:
     * <ul>
     *   <li>Fetching all table IDs from the database</li>
     *   <li>Determining validation scope based on schema storage configuration</li>
     *   <li>Filtering tables according to connector filters</li>
     *   <li>Validating against configured guardrail limits</li>
     * </ul>
     *
     * @param tableIdSupplier Supplier that provides table IDs from the database
     * @param connectorConfig The connector configuration
     * @param tableFilters The table filters for the connector
     * @param storeOnlyCapturedTables Whether only captured tables are stored in schema history
     * @param storeOnlyCapturedDatabases Whether only captured databases are stored in schema history
     * @throws DebeziumException if validation fails or SQLException occurs
     */
    public static void validateTableLimitHistorized(TableIdSupplier tableIdSupplier,
                                                    CommonConnectorConfig connectorConfig,
                                                    RelationalTableFilters tableFilters,
                                                    boolean storeOnlyCapturedTables,
                                                    boolean storeOnlyCapturedDatabases) {

        try {
            Set<TableId> allTableIds = tableIdSupplier.get();
            List<String> tableNames;
            boolean isValidatingAllTables = !storeOnlyCapturedTables;

            if (isValidatingAllTables) {
                tableNames = allTableIds.stream()
                        .filter(tableId -> tableFilters.eligibleForSchemaDataCollectionFilter().isIncluded(tableId))
                        .map(TableId::toString)
                        .collect(Collectors.toList());
                LOGGER.info("Validating guardrail limits against {} tables present in {}",
                        tableNames.size(),
                        storeOnlyCapturedDatabases ? "the captured databases" : "all databases");
            }
            else {
                tableNames = allTableIds.stream()
                        .filter(tableId -> tableFilters.dataCollectionFilter().isIncluded(tableId))
                        .map(TableId::toString)
                        .collect(Collectors.toList());
                LOGGER.info("Validating guardrail limits against {} captured tables", tableNames.size());
            }

            connectorConfig.validateGuardrailLimits(tableNames, isValidatingAllTables);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to validate guardrail limits", e);
        }
    }

    /**
     * Validates guardrail limits for table-based connectors which are non schema-history based.
     * <p>
     * This is a simpler variant used by connectors that don't support the
     * storeOnlyCapturedTables configuration (e.g., PostgreSQL).
     *
     * @param tableIdSupplier Supplier that provides table IDs from the database
     * @param connectorConfig The connector configuration
     * @param dataCollectionFilter The filter to identify captured tables
     * @throws DebeziumException if validation fails or SQLException occurs
     */
    public static void validateTableLimitOnCaptured(TableIdSupplier tableIdSupplier,
                                                    CommonConnectorConfig connectorConfig,
                                                    Predicate<TableId> dataCollectionFilter) {

        try {
            Set<TableId> allTableIds = tableIdSupplier.get();

            List<String> tableNames = allTableIds.stream()
                    .filter(dataCollectionFilter)
                    .map(TableId::toString)
                    .collect(Collectors.toList());

            LOGGER.info("Validating guardrail limits against {} captured tables", tableNames.size());
            connectorConfig.validateGuardrailLimits(tableNames);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to validate guardrail limits", e);
        }
    }

    /**
     * Validates guardrail limits for collection-based connectors which are non schema-history based (e.g., MongoDB).
     * <p>
     * This variant works with collection names instead of TableId objects.
     *
     * @param collectionNames The list of collection names to validate
     * @param connectorConfig The connector configuration
     * @throws DebeziumException if validation fails
     */
    public static void validateCollectionLimitCaptured(Collection<String> collectionNames,
                                                       CommonConnectorConfig connectorConfig) {

        LOGGER.info("Validating guardrail limits against {} captured collections", collectionNames.size());
        connectorConfig.validateGuardrailLimits(collectionNames);
    }

    /**
     * Functional interface for database operations that supply table IDs.
     * Used to defer database operations until inside the try-catch block.
     */
    @FunctionalInterface
    public interface TableIdSupplier {
        /**
         * Gets table IDs from the database.
         *
         * @return a set of table IDs
         * @throws SQLException if a database access error occurs
         */
        Set<TableId> get() throws SQLException;
    }
}
