/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.snapshot.lock;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link ExtendedSnapshotLock}.
 *
 * @author Chris Cranford
 */
public class ExtendedSnapshotLockTest {

    @Test
    @FixFor("debezium/dbz#2787")
    public void shouldIncludeWaitClauseWhenLockTimeoutProvided() {
        final var lock = new ExtendedSnapshotLock();
        assertThat(lock.tableLockingStatement(Duration.ofSeconds(10), "\"DEBEZIUM\".\"TEST\""))
                .contains("LOCK TABLE \"DEBEZIUM\".\"TEST\" IN SHARE MODE WAIT 10");
    }

    @Test
    @FixFor("debezium/dbz#2787")
    public void shouldTruncateLockTimeoutToSeconds() {
        final var lock = new ExtendedSnapshotLock();
        assertThat(lock.tableLockingStatement(Duration.ofMillis(2500), "\"DEBEZIUM\".\"TEST\""))
                .contains("LOCK TABLE \"DEBEZIUM\".\"TEST\" IN SHARE MODE WAIT 2");
    }

    @Test
    @FixFor("debezium/dbz#2787")
    public void shouldUseNoWaitWhenLockTimeoutIsLessThanOneSecond() {
        final var lock = new ExtendedSnapshotLock();
        assertThat(lock.tableLockingStatement(Duration.ZERO, "\"DEBEZIUM\".\"TEST\""))
                .contains("LOCK TABLE \"DEBEZIUM\".\"TEST\" IN SHARE MODE NOWAIT");
        assertThat(lock.tableLockingStatement(Duration.ofMillis(500), "\"DEBEZIUM\".\"TEST\""))
                .contains("LOCK TABLE \"DEBEZIUM\".\"TEST\" IN SHARE MODE NOWAIT");
    }

    @Test
    @FixFor("debezium/dbz#2787")
    public void shouldOmitWaitClauseWhenLockTimeoutIsNegative() {
        final var lock = new ExtendedSnapshotLock();
        assertThat(lock.tableLockingStatement(Duration.ofMillis(-1), "\"DEBEZIUM\".\"TEST\""))
                .contains("LOCK TABLE \"DEBEZIUM\".\"TEST\" IN SHARE MODE");
    }

    @Test
    @FixFor("debezium/dbz#2787")
    public void shouldOmitWaitClauseWhenNoLockTimeoutProvided() {
        final var lock = new ExtendedSnapshotLock();
        assertThat(lock.tableLockingStatement(null, "\"DEBEZIUM\".\"TEST\""))
                .contains("LOCK TABLE \"DEBEZIUM\".\"TEST\" IN SHARE MODE");
    }
}
