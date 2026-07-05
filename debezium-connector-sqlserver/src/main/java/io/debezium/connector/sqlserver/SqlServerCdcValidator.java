/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.TableId;

/**
 * Validator for CDC prerequisites on SQL Server before connector startup.
 * Checks:
 * - CDC is enabled on the database
 * - CDC-enabled tables exist and match configured table filters
 * - SQL Server Agent is running
 *
 * @author Debezium Contributors
 */
public class SqlServerCdcValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerCdcValidator.class);

    private static final String CHECK_CDC_ENABLED_ON_DB = "SELECT is_cdc_enabled FROM sys.databases WHERE name = ?";
    private static final String GET_CDC_ENABLED_TABLES = "SELECT OBJECT_SCHEMA_NAME(t.object_id), t.name FROM sys.tables t WHERE t.is_tracked_by_cdc = 1 AND DATABASE_ID = DB_ID()";
    private static final String GET_SQL_AGENT_STATUS = "SELECT status_desc FROM sys.dm_server_services WHERE servicename LIKE 'SQL Server Agent%'";

    private final SqlServerConnectorConfig config;
    private final SqlServerConnection connection;

    public SqlServerCdcValidator(SqlServerConnectorConfig config, SqlServerConnection connection) {
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.connection = Objects.requireNonNull(connection, "connection cannot be null");
    }

    /**
     * Validates CDC setup for a specific database.
     *
     * @param databaseName the database to validate
     * @return validation result with errors and warnings
     */
    public CdcValidationResult validateCdcSetup(String databaseName) {
        final List<String> errors = new ArrayList<>();
        final List<String> warnings = new ArrayList<>();

        try {
            // Check 1: Is CDC enabled on database?
            if (!isCdcEnabledOnDatabase(databaseName)) {
                errors.add(String.format(
                        "CDC is not enabled on database '%s'. " +
                                "Enable CDC with SQL query: EXEC sys.sp_cdc_enable_db",
                        databaseName));
            }
            else {
                LOGGER.debug("CDC is enabled on database '{}'", databaseName);

                // Check 2: Are there CDC-enabled tables in the database?
                final List<String> cdcEnabledTables = getCdcEnabledTables(databaseName);
                if (cdcEnabledTables.isEmpty()) {
                    errors.add(String.format(
                            "No tables have CDC enabled in database '%s'. " +
                                    "Enable CDC on tables with SQL query: EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='TableName', @role_name=NULL",
                            databaseName));
                }
                else {
                    LOGGER.debug("Found {} CDC-enabled tables in database '{}'", cdcEnabledTables.size(), databaseName);

                    // Check 2b: Do configured tables have CDC enabled?
                    try {
                        checkConfiguredTablesHaveCdc(databaseName, cdcEnabledTables, warnings);
                    }
                    catch (SQLException e) {
                        LOGGER.debug("Error checking configured tables for CDC", e);
                        // Continue validation - this is a secondary check
                    }
                }
            }

            // Check 3: Is SQL Server Agent running? (only warn, not error, as some setups may not need it)
            if (!isSqlServerAgentRunning()) {
                warnings.add("SQL Server Agent is not running. CDC requires SQL Server Agent to be running to capture and populate change data. " +
                        "Start SQL Server Agent or set it to run automatically.");
            }
            else {
                LOGGER.debug("SQL Server Agent is running");
            }

            // Note: Table-level CDC enablement check is done at runtime by SqlServerStreamingChangeEventSource
            // which provides detailed feedback on which tables are missing CDC or not in the include list
        }
        catch (SQLException e) {
            LOGGER.debug("Error during CDC validation", e);
            errors.add("Error validating CDC setup: " + e.getMessage());
        }

        return new CdcValidationResult(errors, warnings);
    }

    /**
     * Checks if CDC is enabled on the database.
     */
    private boolean isCdcEnabledOnDatabase(String databaseName) throws SQLException {
        final String query = "USE [" + databaseName + "]; " + CHECK_CDC_ENABLED_ON_DB;
        return connection.queryAndMap(
                query,
                rs -> rs.next() && rs.getBoolean("is_cdc_enabled"));
    }

    /**
     * Gets list of CDC-enabled tables in the database.
     */
    private List<String> getCdcEnabledTables(String databaseName) throws SQLException {
        final String query = "USE [" + databaseName + "]; " + GET_CDC_ENABLED_TABLES;
        return connection.queryAndMap(
                query,
                rs -> {
                    final List<String> tables = new ArrayList<>();
                    while (rs.next()) {
                        final String schema = rs.getString(1);
                        final String tableName = rs.getString(2);
                        tables.add(schema + "." + tableName);
                    }
                    return tables;
                });
    }

    /**
     * Checks that all user-configured tables have CDC enabled.
     * Only validates CDC on tables that match the user's include/exclude filters.
     */
    private void checkConfiguredTablesHaveCdc(String databaseName, List<String> cdcEnabledTables, List<String> warnings) throws SQLException {
        // Convert CDC-enabled tables to set for fast lookup
        final Set<String> cdcEnabledSet = cdcEnabledTables.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());

        // Get ALL tables in the database
        final Set<TableId> allTables = connection.getAllTableIds(databaseName);

        // Find tables that match user's configured filters but DON'T have CDC enabled
        final List<TableId> configuredTablesWithoutCdc = new ArrayList<>();

        for (TableId tableId : allTables) {
            // Check if this table matches user's include/exclude filters
            if (config.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                // User wants to capture this table
                final String fullTableName = tableId.schema() + "." + tableId.table();

                // Check if this table has CDC enabled
                if (!cdcEnabledSet.contains(fullTableName.toLowerCase())) {
                    configuredTablesWithoutCdc.add(tableId);
                    LOGGER.info("Table {} is configured for capture but does not have CDC enabled", tableId);
                }
                else {
                    LOGGER.debug("Table {} is configured and has CDC enabled", tableId);
                }
            }
        }

        // Error if any configured table lacks CDC
        if (!configuredTablesWithoutCdc.isEmpty()) {
            final String tableList = configuredTablesWithoutCdc.stream()
                    .map(tid -> tid.schema() + "." + tid.table())
                    .collect(Collectors.joining(", "));

            warnings.add(String.format(
                    "The following configured tables do not have CDC enabled: %s. " +
                            "Enable CDC on these tables with: EXEC sys.sp_cdc_enable_table @source_schema='schema_name', @source_name='table_name', @role_name=NULL",
                    tableList));
        }
    }

    /**
     * Checks if SQL Server Agent is running.
     */
    private boolean isSqlServerAgentRunning() throws SQLException {
        return connection.queryAndMap(
                GET_SQL_AGENT_STATUS,
                rs -> {
                    if (rs.next()) {
                        final String status = rs.getString("status_desc");
                        return "Running".equalsIgnoreCase(status);
                    }
                    return false;
                });
    }

    /**
     * Result of CDC validation containing errors and warnings.
     */
    public static class CdcValidationResult {

        private final List<String> errors;
        private final List<String> warnings;

        public CdcValidationResult(List<String> errors, List<String> warnings) {
            this.errors = Objects.requireNonNull(errors, "errors cannot be null");
            this.warnings = Objects.requireNonNull(warnings, "warnings cannot be null");
        }

        public List<String> getErrors() {
            return errors;
        }

        public List<String> getWarnings() {
            return warnings;
        }

        public boolean hasErrors() {
            return !errors.isEmpty();
        }

        public boolean hasWarnings() {
            return !warnings.isEmpty();
        }

        @Override
        public String toString() {
            return "CdcValidationResult{" +
                    "errors=" + errors +
                    ", warnings=" + warnings +
                    '}';
        }
    }
}
