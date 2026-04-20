/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.Scn;

/**
 * The legacy behavior that applies no log caps and uses all logs collected, and preserves the pre-computed
 * upper boundary as the maximum session boundary.
 *
 * @author Chris Cranford
 */
public class UnboundedLogFileSessionSelector implements LogFileSessionSelector {

    private final Logger LOGGER = LoggerFactory.getLogger(UnboundedLogFileSessionSelector.class);

    @Override
    public SessionLogSelection selectLogsForSession(LogFileCollector.LogFilesResult logFilesResult, Scn upperBoundary) {
        LOGGER.debug("Using all logs and reading up to {}.", upperBoundary);
        return new SessionLogSelection(logFilesResult.logFiles(), upperBoundary);
    }
}
