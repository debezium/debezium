/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

/**
 * Listener receiving lifecycle and data events from {@link SchemaHistory}.
 *
 * @author Jiri Pechanec
 *
 */
public interface SchemaHistoryListener {
    void started();

    void stopped();

    void recoveryStarted();

    void recoveryStopped();

    /**
     * Invoked for every change read from the history during recovery.
     *
     * @param record
     */
    void onChangeFromHistory(HistoryRecord record);

    /**
     * Invoked for every change applied and not filtered.
     *
     * @param record
     */
    void onChangeApplied(HistoryRecord record);

    SchemaHistoryListener NOOP = new SchemaHistoryListener() {
        @Override
        public void stopped() {
        }

        @Override
        public void started() {
        }

        @Override
        public void recoveryStopped() {
        }

        @Override
        public void recoveryStarted() {
        }

        @Override
        public void onChangeFromHistory(HistoryRecord record) {
        }

        @Override
        public void onChangeApplied(HistoryRecord record) {
        }
    };
}
