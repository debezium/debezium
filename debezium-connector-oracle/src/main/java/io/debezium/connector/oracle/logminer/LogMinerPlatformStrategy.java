/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.util.List;

import io.debezium.connector.oracle.Scn;

/**
 * Strategy interface that abstracts Oracle LogMiner PL/SQL operations and V$ view queries.
 *
 * <p>Different Oracle deployment platforms (standard on-premise, AWS RDS, OCI) may require
 * different PL/SQL packages or SQL queries to interact with LogMiner. For example, AWS RDS
 * with CDB deployments does not permit direct access to CDB$ROOT and provides custom
 * PL/SQL packages via {@code rdsadmin} for LogMiner operations.
 *
 * <p>Implementations of this interface encapsulate the platform-specific SQL/PL/SQL calls
 * so the rest of the connector can remain platform-agnostic. Strategy methods return SQL
 * strings rather than executing them directly, allowing the caller to control execution.
 *
 * @author Chris Cranford
 */
public interface LogMinerPlatformStrategy {

    /**
     * Get the SQL to add a log file to the current LogMiner session.
     *
     * @param fileName the fully qualified log file name
     * @return the SQL string to execute
     */
    String getAddLogFileSql(String fileName);

    /**
     * Get the SQL to start a LogMiner mining session.
     *
     * @param startScn the starting system change number, may be {@link Scn#NULL}
     * @param endScn the ending system change number, may be {@link Scn#NULL}
     * @param miningOptions the comma-separated mining options string
     * @param dictionaryFilePath the dictionary file path, may be {@code null}
     * @return the SQL string to execute
     */
    String getStartSessionSql(Scn startScn, Scn endScn, String miningOptions, String dictionaryFilePath);

    /**
     * Get the SQL to end the current LogMiner mining session.
     *
     * @return the SQL string to execute
     */
    String getEndSessionSql();

    /**
     * Get the SQL to write the data dictionary to the Oracle transaction redo logs.
     *
     * @return the SQL string to execute
     */
    String getWriteDataDictionaryToRedoLogsSql();

    /**
     * Get the SQL to remove a specific log file from the current LogMiner session.
     *
     * @param fileName the fully qualified log file name
     * @return the SQL string to execute
     */
    String getRemoveLogFileSql(String fileName);

    /**
     * Determine whether direct access to the CDB root container is supported.
     *
     * <p>On standard Oracle deployments, switching to CDB$ROOT via
     * {@code ALTER SESSION SET CONTAINER=cdb$root} is permitted. On AWS RDS
     * CDB deployments, this is blocked and the connector must operate without
     * CDB root access.
     *
     * @return {@code true} if CDB root access is available, {@code false} otherwise
     */
    boolean isCdbRootAccessible();

    /**
     * Get the SQL query to retrieve the current SCN.
     *
     * @return the SQL query string
     */
    String getCurrentScnQuery();

    /**
     * Get the SQL query to check whether the database is in ARCHIVELOG mode.
     *
     * @return the SQL query string
     */
    String getArchiveLogModeQuery();

    /**
     * Get the SQL query to retrieve the redo thread state.
     *
     * @return the SQL query string
     */
    String getRedoThreadStateQuery();

    /**
     * Get the SQL query to retrieve the list of log file names currently registered
     * in the LogMiner session.
     *
     * @return the SQL query string
     */
    String getRegisteredLogFilesQuery();

    /**
     * Get the list of mining option constants for a given strategy and configuration.
     *
     * @param options the list of options to populate
     */
    default void addMiningOptionConstants(List<String> options) {
        // no-op by default
    }
}
