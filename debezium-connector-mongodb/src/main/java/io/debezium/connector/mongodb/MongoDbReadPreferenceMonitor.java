/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;

import io.debezium.util.Clock;

/**
 * Detects when an open change stream cursor no longer satisfies its configured read preference.
 */
final class MongoDbReadPreferenceMonitor {

    enum Status {
        SATISFIED,
        RELOCATE,
        NO_ELIGIBLE_SERVER,
        UNVERIFIED
    }

    private final ReadPreference readPreference;
    private final long checkIntervalMs;
    private final long checkIntervalNanos;
    private final Clock clock;
    private long lastCheckTimeNanos;

    MongoDbReadPreferenceMonitor(ReadPreference readPreference, long checkIntervalMs, Clock clock) {
        this.readPreference = Objects.requireNonNull(readPreference);
        this.checkIntervalMs = Math.max(1, checkIntervalMs);
        this.checkIntervalNanos = TimeUnit.MILLISECONDS.toNanos(this.checkIntervalMs);
        this.clock = Objects.requireNonNull(clock);
        this.lastCheckTimeNanos = clock.currentTimeInNanos();
    }

    Status evaluate(ClusterDescription clusterDescription, ServerAddress cursorAddress) {
        if (clusterDescription.getType() != ClusterType.REPLICA_SET
                || clusterDescription.getConnectionMode() != ClusterConnectionMode.MULTIPLE) {
            return Status.SATISFIED;
        }

        var cursorServer = clusterDescription.getServerDescriptions().stream()
                .filter(server -> server.getAddress().equals(cursorAddress))
                .filter(server -> server.isOk())
                .findFirst();

        if (cursorServer.isEmpty()) {
            return Status.UNVERIFIED;
        }

        var candidates = readPreference.choose(clusterDescription);
        if (candidates.stream().anyMatch(server -> server.getAddress().equals(cursorAddress))) {
            return Status.SATISFIED;
        }

        return candidates.isEmpty() ? Status.NO_ELIGIBLE_SERVER : Status.RELOCATE;
    }

    boolean shouldWaitForEligibleServer(ClusterDescription clusterDescription) {
        if (clusterDescription.getConnectionMode() != ClusterConnectionMode.MULTIPLE) {
            return false;
        }

        if (clusterDescription.getType() == ClusterType.UNKNOWN) {
            return true;
        }

        return clusterDescription.getType() == ClusterType.REPLICA_SET
                && readPreference.choose(clusterDescription).isEmpty();
    }

    ReadPreference getReadPreference() {
        return readPreference;
    }

    long getCheckIntervalMs() {
        return checkIntervalMs;
    }

    boolean isCheckDue() {
        var now = clock.currentTimeInNanos();
        if (now - lastCheckTimeNanos < checkIntervalNanos) {
            return false;
        }

        lastCheckTimeNanos = now;
        return true;
    }
}
