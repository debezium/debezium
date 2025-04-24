/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

/**
 * Contract used by LogMiner adapters to build a LogMiner query.
 *
 * @author Chris Cranford
 */
public interface LogMinerQueryBuilder {
    /**
     * @return a query to fetch data from LogMiner based on the connector configuration
     */
    String getQuery();
}
