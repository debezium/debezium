/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.snapshot.lock;

/**
 * Contract for {@link io.debezium.snapshot.spi.SnapshotLock} implementations whose global lock is not
 * released by the default {@code UNLOCK TABLES} statement, e.g. the MySQL instance-level backup lock
 * that requires {@code UNLOCK INSTANCE}.
 */
public interface UnlockStatementProvider {

    /**
     * @return the statement that releases the global lock acquired by the locking statement; never null
     */
    String globalUnlockingStatement();
}
