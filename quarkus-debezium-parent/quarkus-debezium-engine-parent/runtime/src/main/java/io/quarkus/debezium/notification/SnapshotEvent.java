/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.notification;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public sealed abstract class SnapshotEvent implements Notification
        permits SnapshotStarted, SnapshotInProgress, SnapshotTableScanCompleted, SnapshotAborted, SnapshotSkipped, SnapshotCompleted, SnapshotPaused, SnapshotResumed {

    private final String id;
    private final Map<String, String> additionalData;
    private final Long timestamp;
    private final Kind kind;

    protected SnapshotEvent(String id, Map<String, String> additionalData, Long timestamp, Kind kind) {
        this.id = id;
        this.additionalData = additionalData;
        this.timestamp = timestamp;
        this.kind = kind;
    }

    public String getId() {
        return id;
    }

    public Map<String, String> getAdditionalData() {
        return additionalData;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Kind getKind() {
        return kind;
    }

    public enum Kind {
        INCREMENTAL("Incremental Snapshot"),
        INITIAL("Initial Snapshot");

        private final String description;

        Kind(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }

        public static Optional<Kind> from(String value) {
            return Arrays.stream(Kind.values())
                    .filter(kind -> kind.getDescription().equals(value))
                    .findFirst();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SnapshotEvent that = (SnapshotEvent) o;
        return Objects.equals(id, that.id) && Objects.equals(additionalData, that.additionalData) && Objects.equals(timestamp, that.timestamp) && kind == that.kind;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, additionalData, timestamp, kind);
    }
}
