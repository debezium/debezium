/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector;

/**
 * Describes the kind of snapshot in progress
 *
 * @author Mario Fiore Vitale
 *
 */
public enum SnapshotType {
    /**
     * Indicates it is an initial snapshot.
     */
    INITIAL,
    /**
     * Indicates it is a blocking snapshot.
     */
    BLOCKING
}
