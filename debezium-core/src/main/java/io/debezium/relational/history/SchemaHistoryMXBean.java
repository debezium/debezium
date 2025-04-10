/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

/**
 * Metrics describing {@link SchemaHistory} use.
 * @author Jiri Pechanec
 *
 */
public interface SchemaHistoryMXBean {

    /**
     * The schema history starts in {@code STOPPED} state.
     * Upon start it transitions to {@code RECOVERING} state.
     * When all changes from stored history were applied then it switches to {@code RUNNING} state.
     * <p>Maps to {@link SchemaHistoryMetrics.SchemaHistoryStatus} enum.
     *
     * @return schema history component state
     */
    long getStatus();

    /**
     * @return time in epoch seconds when recovery has started
     */
    long getRecoveryStartTime();

    /**
     * @return number of changes that were read during recovery phase
     */
    long getChangesRecovered();

    /**
     * @return number of changes that were applied during recovery phase increased by number of changes
     * applied during runtime
     */
    long getChangesApplied();

    /**
     * @return elapsed time in milliseconds since the last change was applied
     */
    long getMilliSecondsSinceLastAppliedChange();

    /**
     * @return elapsed time in milliseconds since the last record was recovered from history
     */
    long getMilliSecondsSinceLastRecoveredChange();

    /**
     * @return String representation of the last applied change
     */
    String getLastAppliedChange();

    /**
     * @return String representation of the last recovered change
     */
    String getLastRecoveredChange();
}
