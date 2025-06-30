/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.notification;

import java.util.Map;

public final class SnapshotResumed extends SnapshotEvent {
    SnapshotResumed(String id, Map<String, String> additionalData, Long timestamp, Kind kind) {
        super(id, additionalData, timestamp, kind);
    }
}
