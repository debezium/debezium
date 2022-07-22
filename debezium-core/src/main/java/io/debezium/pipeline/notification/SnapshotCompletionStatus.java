/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification;

public enum SnapshotCompletionStatus {
    EMPTY,
    NO_PRIMARY_KEY,
    SQL_EXCEPTION,
    STOPPED,
    SUCCEEDED,
    UNKNOWN_SCHEMA
}
