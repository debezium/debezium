/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.Scn;

/**
 * A specialized exception that signals that the consumption of a LogMiner event's SQL
 * has reached a defined maximum and cannot be ingested.
 *
 * @author Chris Cranford
 */
public class LogMinerEventRowTooLargeException extends DebeziumException {
    public LogMinerEventRowTooLargeException(String tableName, Long limitInKiloBytes, Scn scn) {
        super(String.format("A row has been found for table %s that exceeds %dKb with SCN %s.", tableName, limitInKiloBytes, scn));
    }
}
