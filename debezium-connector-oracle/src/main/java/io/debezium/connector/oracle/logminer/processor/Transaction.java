/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;

/**
 * Contract for an Oracle transaction.
 *
 * @author Chris Cranford
 */
public interface Transaction {

    String getTransactionId();

    Scn getStartScn();

    Instant getChangeTime();

    int getNumberOfEvents();

    int getNextEventId();

    void started();
}
