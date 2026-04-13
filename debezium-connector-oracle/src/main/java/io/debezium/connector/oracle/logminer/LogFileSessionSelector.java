/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.util.List;

import io.debezium.connector.oracle.Scn;

/**
 * Defines the contract for the selector that computes what logs should be added to the LogMiner session.
 *
 * @author Chris Cranford
 */
public interface LogFileSessionSelector {
    /**
     * The selected logs based on the selector strategy.
     *
     * @param logFiles the log files selected, never {@code null}
     * @param effectiveUpperBounds the effective upper boundary for the mining session, never {@code null}
     */
    record SessionLogSelection(List<LogFile> logFiles, Scn effectiveUpperBounds) {
    }

    /**
     * Selects logs for the LogMiner mining session.
     *
     * @param logFilesResult the collected log files result object, should not be {@code null}
     * @param upperBoundary the pre-computed upper boundary of the system, should not be {@code null}
     * @return the selected logs and boundary based on the selector implementation
     */
    SessionLogSelection selectLogsForSession(LogFileCollector.LogFilesResult logFilesResult, Scn upperBoundary);
}
