/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import static org.fest.assertions.Assertions.assertThat;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.embedded.spi.OffsetCommitPolicy;

/**
 * @author Randall Hauch
 */
public class OffsetCommitPolicyTest {

    @Test
    public void shouldAlwaysCommit() {
        OffsetCommitPolicy policy = OffsetCommitPolicy.always();
        assertThat(policy.performCommit(0, Duration.ofNanos(0))).isTrue();
        assertThat(policy.performCommit(10000, Duration.ofDays(1000))).isTrue();
    }

    @Test
    public void shouldCommitPeriodically() {
        // 10 hours
        OffsetCommitPolicy policy = OffsetCommitPolicy.periodic(Configuration.create().with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 10 * 60 * 60 * 1000).build());
        assertThat(policy.performCommit(0, Duration.ofNanos(0))).isFalse();
        assertThat(policy.performCommit(10000, Duration.ofHours(9))).isFalse();
        assertThat(policy.performCommit(0, Duration.ofHours(10))).isTrue();
    }

    @Test
    public void shouldCommitPeriodicallyWithProperties() {
        // 10 hours
        final Properties props = Configuration.create().with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 10 * 60 * 60 * 1000).build().asProperties();
        OffsetCommitPolicy policy = new OffsetCommitPolicy.PeriodicCommitOffsetPolicy(props);
        assertThat(policy.performCommit(0, Duration.ofNanos(0))).isFalse();
        assertThat(policy.performCommit(10000, Duration.ofHours(9))).isFalse();
        assertThat(policy.performCommit(0, Duration.ofHours(10))).isTrue();
    }

    @Test
    public void shouldCombineTwoPolicies() {
        AtomicBoolean commitFirst = new AtomicBoolean(false);
        AtomicBoolean commitSecond = new AtomicBoolean(false);
        OffsetCommitPolicy policy1 = (num, time) -> commitFirst.get();
        OffsetCommitPolicy policy2 = (num, time) -> commitSecond.get();
        OffsetCommitPolicy both1 = policy1.and(policy2);
        OffsetCommitPolicy both2 = policy2.and(policy1);
        OffsetCommitPolicy either1 = policy1.or(policy2);
        OffsetCommitPolicy either2 = policy2.or(policy1);

        assertThat(both1.performCommit(0, Duration.ofNanos(0))).isFalse();
        assertThat(both2.performCommit(0, Duration.ofNanos(0))).isFalse();
        assertThat(either1.performCommit(0, Duration.ofNanos(0))).isFalse();
        assertThat(either2.performCommit(0, Duration.ofNanos(0))).isFalse();

        commitFirst.set(true);
        assertThat(both1.performCommit(0, Duration.ofNanos(0))).isFalse();
        assertThat(both2.performCommit(0, Duration.ofNanos(0))).isFalse();
        assertThat(either1.performCommit(0, Duration.ofNanos(0))).isTrue();
        assertThat(either2.performCommit(0, Duration.ofNanos(0))).isTrue();

        commitSecond.set(true);
        assertThat(both1.performCommit(0, Duration.ofNanos(0))).isTrue();
        assertThat(both2.performCommit(0, Duration.ofNanos(0))).isTrue();
        assertThat(either1.performCommit(0, Duration.ofNanos(0))).isTrue();
        assertThat(either2.performCommit(0, Duration.ofNanos(0))).isTrue();

        commitFirst.set(false);
        assertThat(both1.performCommit(0, Duration.ofNanos(0))).isFalse();
        assertThat(both2.performCommit(0, Duration.ofNanos(0))).isFalse();
        assertThat(either1.performCommit(0, Duration.ofNanos(0))).isTrue();
        assertThat(either2.performCommit(0, Duration.ofNanos(0))).isTrue();

        commitSecond.set(false);
        assertThat(both1.performCommit(0, Duration.ofNanos(0))).isFalse();
        assertThat(both2.performCommit(0, Duration.ofNanos(0))).isFalse();
        assertThat(either1.performCommit(0, Duration.ofNanos(0))).isFalse();
        assertThat(either2.performCommit(0, Duration.ofNanos(0))).isFalse();
    }

    @Test
    public void shouldCombineOnePolicyWithNull() {
        AtomicBoolean commit = new AtomicBoolean(false);
        OffsetCommitPolicy policy1 = (num, time) -> commit.get();
        assertThat(policy1.and(null)).isSameAs(policy1);
        assertThat(policy1.or(null)).isSameAs(policy1);
    }

}
