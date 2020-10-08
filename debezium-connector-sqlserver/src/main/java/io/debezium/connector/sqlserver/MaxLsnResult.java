/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

/**
 * Stores the result from querying the lsn_time_mapping for the highest LSNs.
 */
public class MaxLsnResult {

    /**
     * The highest lsn regardless of configuration. This should be utilized when querying the CDC tables as using an LSN that is no longer valid can lead to errors.
     */
    private final Lsn maxLsn;

    /**
     * The highest lsn belonging to a valid change transaction as determined by {@link SqlServerConnectorConfig#MAX_LSN_OPTIMIZATION} or default to the same as maxLsn.
     */
    private final Lsn maxTransactionalLsn;

    public MaxLsnResult(Lsn maxLsn, Lsn maxTransactionalLsn) {
        this.maxLsn = maxLsn;
        this.maxTransactionalLsn = maxTransactionalLsn;
    }

    public Lsn getMaxLsn() {
        return maxLsn;
    }

    public Lsn getMaxTransactionalLsn() {
        return maxTransactionalLsn;
    }
}
