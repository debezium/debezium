/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.snapshot.lock;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotLockingMode;

public class MinimalInstanceSnapshotLockTest {

    @Test
    public void shouldUseInstanceBackupLockStatements() {
        final MinimalInstanceSnapshotLock lock = new MinimalInstanceSnapshotLock();

        assertThat(lock.name()).isEqualTo(SnapshotLockingMode.MINIMAL_INSTANCE.getValue());
        assertThat(lock.tableLockingStatement(null, null)).contains("LOCK INSTANCE FOR BACKUP");
        assertThat(lock.globalUnlockingStatement()).isEqualTo("UNLOCK INSTANCE");
    }

    @Test
    public void modeFlagsShouldMatchBackupLockSemantics() {
        final SnapshotLockingMode mode = SnapshotLockingMode.MINIMAL_INSTANCE;

        // lock is held only for the initial schema portion of the snapshot
        assertThat(mode.usesLocking()).isTrue();
        assertThat(mode.usesMinimalLocking()).isTrue();
        // no FLUSH is executed, so the isolation level does not need to be restored
        assertThat(mode.flushResetsIsolationLevel()).isFalse();
        assertThat(mode.useConsistentSnapshotTransaction()).isTrue();
        // table-level lock fallback stays available, mirroring minimal_percona
        assertThat(mode.preventsTableLocks()).isFalse();
    }
}
