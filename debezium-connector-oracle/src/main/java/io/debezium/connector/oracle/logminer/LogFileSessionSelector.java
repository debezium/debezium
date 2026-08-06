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

    /**
     * Gets the collected logs restricted to the consistent mining window.
     * <p>
     * When the collector truncated the consistency range at a log sequence gap, logs that start at or
     * beyond the consistent boundary must not be added to the mining session; otherwise the full
     * collected log list is returned unchanged.
     *
     * @param logFilesResult the collected log files result object, should not be {@code null}
     * @return the logs eligible for the mining session, never {@code null}
     */
    default List<LogFile> getConsistentLogFiles(LogFileCollector.LogFilesResult logFilesResult) {
        final Scn consistentThroughScn = logFilesResult.consistentThroughScn();
        if (consistentThroughScn.isNull()) {
            return logFilesResult.logFiles();
        }
        return logFilesResult.logFiles().stream()
                .filter(log -> log.getFirstScn().compareTo(consistentThroughScn) < 0)
                .toList();
    }
}
