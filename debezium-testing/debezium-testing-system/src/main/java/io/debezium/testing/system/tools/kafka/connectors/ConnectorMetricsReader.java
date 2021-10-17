/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka.connectors;

public interface ConnectorMetricsReader {

    /**
     * Waits until snapshot phase of given MySQL connector completes
     *
     * @param connectorName connector name
     */
    void waitForMySqlSnapshot(String connectorName);

    /**
     * Waits until snapshot phase of given PostgreSQL connector completes
     *
     * @param connectorName connector name
     */
    void waitForPostgreSqlSnapshot(String connectorName);

    /**
     * Waits until snapshot phase of given SQL Server connector completes
     *
     * @param connectorName connector name
     */
    void waitForSqlServerSnapshot(String connectorName);

    /**
     * Waits until snapshot phase of given MongoDB connector completes
     *
     * @param connectorName connector name
     */
    void waitForMongoSnapshot(String connectorName);

    /**
     * Waits until snapshot phase of given DB2 connector completes
     *
     * @param connectorName connector name
     */
    void waitForDB2Snapshot(String connectorName);

    /**
     * Waits until snapshot phase of given Oracle connector completes
     *
     * @param connectorName connector name
     */
    void waitForOracleSnapshot(String connectorName);
}
