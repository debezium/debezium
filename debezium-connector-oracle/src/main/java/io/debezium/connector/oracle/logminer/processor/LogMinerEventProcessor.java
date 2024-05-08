/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import java.sql.SQLException;
import java.time.Duration;

import io.debezium.connector.oracle.Scn;

/**
 * Contract that defines the interface for processing events from Oracle LogMiner.
 *
 * @author Chris Cranford
 */
public interface LogMinerEventProcessor extends AutoCloseable {
    /**
     * Process Oracle LogMiner events for a given system change number range.
     *
     * @param startScn the starting system change number, must not be {@code null}
     * @param endScn the ending system change number, must not be {@code null}
     * @return the next iteration's starting system change number, never {@code null}
     */
    Scn process(Scn startScn, Scn endScn) throws SQLException, InterruptedException;

    /**
     * A callback for the event processor to abandon long running transactions.
     *
     * @param retention the maximum duration in which long running transactions are allowed.
     */
    void abandonTransactions(Duration retention) throws InterruptedException;

}
