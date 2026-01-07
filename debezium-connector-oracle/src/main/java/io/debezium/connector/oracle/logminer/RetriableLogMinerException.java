/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.DebeziumException;

/**
 * An exception thrown when an Oracle LogMiner operation can be retried.
 *
 * @author Chris Cranford
 */
public class RetriableLogMinerException extends DebeziumException {
    public RetriableLogMinerException(Throwable throwable) {
        super("LogMiner session failed to start but can be retried", throwable);
    }
}
