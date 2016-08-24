/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.storage.OffsetBackingStore;

/**
 * The policy that defines when the offsets should be committed to {@link OffsetBackingStore offset storage}.
 * 
 * @author Randall Hauch
 */
@FunctionalInterface
interface OffsetCommitPolicy {

    /**
     * Get an {@link OffsetCommitPolicy} that will commit offsets as frequently as possible. This may result in reduced
     * performance, but it has the least potential for seeing source records more than once upon restart.
     * 
     * @return the offset commit policy; never null
     */
    static OffsetCommitPolicy always() {
        return (number, time, unit) -> true;
    }

    /**
     * Get an {@link OffsetCommitPolicy} that will commit offsets no more than the specified time period. If the {@code minimumTime}
     * is not positive, then this method returns {@link #always()}.
     * 
     * @param minimumTime the minimum amount of time between committing offsets
     * @param timeUnit the time unit for {@code minimumTime}; may not be null
     * @return the offset commit policy; never null
     */
    static OffsetCommitPolicy periodic(long minimumTime, TimeUnit timeUnit) {
        if ( minimumTime <= 0 ) return always();
        return (number, actualTime, actualUnit) -> {
            return timeUnit.convert(actualTime, actualUnit) >= minimumTime;
        };
    }

    /**
     * Determine if a commit of the offsets should be performed.
     * 
     * @param numberOfMessagesSinceLastCommit the number of messages that have been received from the connector since last
     *            the offsets were last committed; never negative
     * @param timeSinceLastCommit the time that has elapsed since the offsets were last committed; never negative
     * @param timeUnit the unit of time used for {@code timeSinceLastCommit}; never null
     * @return {@code true} if the offsets should be committed, or {@code false} otherwise
     */
    boolean performCommit(long numberOfMessagesSinceLastCommit, long timeSinceLastCommit, TimeUnit timeUnit);

    /**
     * Obtain a new {@link OffsetCommitPolicy} that will commit offsets if this policy OR the other requests it.
     * 
     * @param other the other commit policy; if null, then this policy instance is returned as is
     * @return the resulting policy; never null
     */
    default OffsetCommitPolicy or(OffsetCommitPolicy other) {
        if ( other == null ) return this;
        return (number, time, unit) -> this.performCommit(number, time, unit) || other.performCommit(number, time, unit);
    }

    /**
     * Obtain a new {@link OffsetCommitPolicy} that will commit offsets if both this policy AND the other requests it.
     * 
     * @param other the other commit policy; if null, then this policy instance is returned as is
     * @return the resulting policy; never null
     */
    default OffsetCommitPolicy and(OffsetCommitPolicy other) {
        if ( other == null ) return this;
        return (number, time, unit) -> this.performCommit(number, time, unit) && other.performCommit(number, time, unit);
    }
}
