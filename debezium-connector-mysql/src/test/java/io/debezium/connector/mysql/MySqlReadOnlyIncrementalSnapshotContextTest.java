/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.mysql.gtid.MySqlGtidSet;

public class MySqlReadOnlyIncrementalSnapshotContextTest {

    private static final String UUID1 = "24bc7850-2c16-11e6-a073-0242ac110002";

    private MySqlReadOnlyIncrementalSnapshotContext<Object> context;

    @BeforeEach
    void setUp() {
        context = new MySqlReadOnlyIncrementalSnapshotContext<>();
    }

    /**
     * Sets low and high watermarks. {@code setHighWatermark} internally subtracts the low, so
     * {@code highGtids} must be a superset of {@code lowGtids} for the subtraction to leave a
     * non-empty result.
     */
    private void setWatermarks(final String lowGtids, final String highGtids) {
        context.setLowWatermark(new MySqlGtidSet(lowGtids));
        context.setHighWatermark(new MySqlGtidSet(highGtids));
    }

    @Test
    void nullHighWatermarkReturnsFalse() {
        // No watermarks set — highWatermark is null.
        assertThat(context.reachedHighWatermark(UUID1 + ":5")).isFalse();
    }

    @Test
    void nullCurrentGtidReturnsTrue() {
        setWatermarks(UUID1 + ":1-5", UUID1 + ":1-10");
        // null means no more events — treated as having passed the watermark.
        assertThat(context.reachedHighWatermark(null)).isTrue();
    }

    @Test
    void untaggedGtidAtHighWatermarkBoundaryReturnsTrue() {
        // high = 1-10 minus low 1-5 -> effective high watermark is 6-10, max=10.
        setWatermarks(UUID1 + ":1-5", UUID1 + ":1-10");
        assertThat(context.reachedHighWatermark(UUID1 + ":10")).isTrue();
    }

    @Test
    void untaggedGtidBeyondHighWatermarkBoundaryReturnsTrue() {
        setWatermarks(UUID1 + ":1-5", UUID1 + ":1-10");
        assertThat(context.reachedHighWatermark(UUID1 + ":11")).isTrue();
    }

    @Test
    void untaggedGtidBelowHighWatermarkBoundaryReturnsFalse() {
        setWatermarks(UUID1 + ":1-5", UUID1 + ":1-10");
        // txId=5 is below the watermark max of 10.
        assertThat(context.reachedHighWatermark(UUID1 + ":5")).isFalse();
    }

    @Test
    void unknownServerIdReturnsFalse() {
        setWatermarks(UUID1 + ":1-5", UUID1 + ":1-10");
        final String otherId = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
        // UUID not in watermark — should return false without throwing.
        assertThat(context.reachedHighWatermark(otherId + ":5")).isFalse();
    }

    @Test
    void taggedGtidAtHighWatermarkBoundaryReturnsTrue() {
        final String tag = "mytag";
        // high = tag:1-10 minus low tag:1-5 -> effective high watermark is tag:6-10, max=10.
        setWatermarks(UUID1 + ":" + tag + ":1-5", UUID1 + ":" + tag + ":1-10");
        assertThat(context.reachedHighWatermark(UUID1 + ":" + tag + ":10")).isTrue();
    }

    @Test
    void taggedGtidBelowHighWatermarkBoundaryReturnsFalse() {
        final String tag = "mytag";
        setWatermarks(UUID1 + ":" + tag + ":1-5", UUID1 + ":" + tag + ":1-10");
        assertThat(context.reachedHighWatermark(UUID1 + ":" + tag + ":5")).isFalse();
    }

    @Test
    void taggedGtidDoesNotMatchUntaggedWatermark() {
        // Watermarks are untagged; GTID carries a tag — lookup should return null, not throw.
        setWatermarks(UUID1 + ":1-5", UUID1 + ":1-10");
        assertThat(context.reachedHighWatermark(UUID1 + ":mytag:5")).isFalse();
    }

    @Test
    void untaggedGtidDoesNotMatchTaggedWatermark() {
        final String tag = "mytag";
        // Watermarks are tagged; GTID is untagged — lookup should return null, not throw.
        setWatermarks(UUID1 + ":" + tag + ":1-5", UUID1 + ":" + tag + ":1-10");
        assertThat(context.reachedHighWatermark(UUID1 + ":10")).isFalse();
    }
}
