/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 */
public class OffsetCommitPolicyTest {

    @Test
    public void shouldAlwaysCommit() {
        OffsetCommitPolicy policy = OffsetCommitPolicy.always();
        assertThat(policy.performCommit(0, 0, TimeUnit.NANOSECONDS)).isTrue();
        assertThat(policy.performCommit(10000, 1000, TimeUnit.DAYS)).isTrue();
    }

    @Test
    public void shouldCommitPeriodically() {
        OffsetCommitPolicy policy = OffsetCommitPolicy.periodic(10, TimeUnit.HOURS);
        assertThat(policy.performCommit(0, 0, TimeUnit.NANOSECONDS)).isFalse();
        assertThat(policy.performCommit(10000, 9, TimeUnit.HOURS)).isFalse();
        assertThat(policy.performCommit(0, 10, TimeUnit.HOURS)).isTrue();
    }

    @Test
    public void shouldCombineTwoPolicies() {
        AtomicBoolean commitFirst = new AtomicBoolean(false);
        AtomicBoolean commitSecond = new AtomicBoolean(false);
        OffsetCommitPolicy policy1 = (num, time, unit) -> commitFirst.get();
        OffsetCommitPolicy policy2 = (num, time, unit) -> commitSecond.get();
        OffsetCommitPolicy both1 = policy1.and(policy2);
        OffsetCommitPolicy both2 = policy2.and(policy1);
        OffsetCommitPolicy either1 = policy1.or(policy2);
        OffsetCommitPolicy either2 = policy2.or(policy1);

        assertThat(both1.performCommit(0, 0, TimeUnit.NANOSECONDS)).isFalse();
        assertThat(both2.performCommit(0, 0, TimeUnit.NANOSECONDS)).isFalse();
        assertThat(either1.performCommit(0, 0, TimeUnit.NANOSECONDS)).isFalse();
        assertThat(either2.performCommit(0, 0, TimeUnit.NANOSECONDS)).isFalse();

        commitFirst.set(true);
        assertThat(both1.performCommit(0, 0, TimeUnit.NANOSECONDS)).isFalse();
        assertThat(both2.performCommit(0, 0, TimeUnit.NANOSECONDS)).isFalse();
        assertThat(either1.performCommit(0, 0, TimeUnit.NANOSECONDS)).isTrue();
        assertThat(either2.performCommit(0, 0, TimeUnit.NANOSECONDS)).isTrue();

        commitSecond.set(true);
        assertThat(both1.performCommit(0, 0, TimeUnit.NANOSECONDS)).isTrue();
        assertThat(both2.performCommit(0, 0, TimeUnit.NANOSECONDS)).isTrue();
        assertThat(either1.performCommit(0, 0, TimeUnit.NANOSECONDS)).isTrue();
        assertThat(either2.performCommit(0, 0, TimeUnit.NANOSECONDS)).isTrue();

        commitFirst.set(false);
        assertThat(both1.performCommit(0, 0, TimeUnit.NANOSECONDS)).isFalse();
        assertThat(both2.performCommit(0, 0, TimeUnit.NANOSECONDS)).isFalse();
        assertThat(either1.performCommit(0, 0, TimeUnit.NANOSECONDS)).isTrue();
        assertThat(either2.performCommit(0, 0, TimeUnit.NANOSECONDS)).isTrue();

        commitSecond.set(false);
        assertThat(both1.performCommit(0, 0, TimeUnit.NANOSECONDS)).isFalse();
        assertThat(both2.performCommit(0, 0, TimeUnit.NANOSECONDS)).isFalse();
        assertThat(either1.performCommit(0, 0, TimeUnit.NANOSECONDS)).isFalse();
        assertThat(either2.performCommit(0, 0, TimeUnit.NANOSECONDS)).isFalse();
    }

    @Test
    public void shouldCombineOnePolicyWithNull() {
        AtomicBoolean commit = new AtomicBoolean(false);
        OffsetCommitPolicy policy1 = (num, time, unit) -> commit.get();
        assertThat(policy1.and(null)).isSameAs(policy1);
        assertThat(policy1.or(null)).isSameAs(policy1);
    }

}
