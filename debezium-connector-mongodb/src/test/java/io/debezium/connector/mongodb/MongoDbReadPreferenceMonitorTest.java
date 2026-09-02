/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.connector.mongodb.MongoDbReadPreferenceMonitor.Status.NO_ELIGIBLE_SERVER;
import static io.debezium.connector.mongodb.MongoDbReadPreferenceMonitor.Status.RELOCATE;
import static io.debezium.connector.mongodb.MongoDbReadPreferenceMonitor.Status.SATISFIED;
import static io.debezium.connector.mongodb.MongoDbReadPreferenceMonitor.Status.UNVERIFIED;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;

import io.debezium.util.Clock;

class MongoDbReadPreferenceMonitorTest {

    private static final ServerAddress SERVER_A = new ServerAddress("server-a", 27017);
    private static final ServerAddress SERVER_B = new ServerAddress("server-b", 27017);

    @Test
    void shouldDetectPrimaryCursorDemotion() {
        var monitor = monitor(ReadPreference.primary());
        var cluster = replicaSet(
                server(SERVER_A, ServerType.REPLICA_SET_SECONDARY),
                server(SERVER_B, ServerType.REPLICA_SET_PRIMARY));

        assertThat(monitor.evaluate(cluster, SERVER_A)).isEqualTo(RELOCATE);
        assertThat(monitor.evaluate(cluster, SERVER_B)).isEqualTo(SATISFIED);
    }

    @Test
    void shouldDetectSecondaryCursorPromotion() {
        var monitor = monitor(ReadPreference.secondary());
        var cluster = replicaSet(
                server(SERVER_A, ServerType.REPLICA_SET_PRIMARY),
                server(SERVER_B, ServerType.REPLICA_SET_SECONDARY));

        assertThat(monitor.evaluate(cluster, SERVER_A)).isEqualTo(RELOCATE);
        assertThat(monitor.evaluate(cluster, SERVER_B)).isEqualTo(SATISFIED);
    }

    @Test
    void shouldHonorPreferredModeFallbacks() {
        var primaryPreferred = monitor(ReadPreference.primaryPreferred());
        var secondaryPreferred = monitor(ReadPreference.secondaryPreferred());
        var cluster = replicaSet(
                server(SERVER_A, ServerType.REPLICA_SET_PRIMARY),
                server(SERVER_B, ServerType.REPLICA_SET_SECONDARY));

        assertThat(primaryPreferred.evaluate(cluster, SERVER_B)).isEqualTo(RELOCATE);
        assertThat(secondaryPreferred.evaluate(cluster, SERVER_A)).isEqualTo(RELOCATE);

        assertThat(primaryPreferred.evaluate(
                replicaSet(server(SERVER_B, ServerType.REPLICA_SET_SECONDARY)), SERVER_B)).isEqualTo(SATISFIED);
        assertThat(secondaryPreferred.evaluate(
                replicaSet(server(SERVER_A, ServerType.REPLICA_SET_PRIMARY)), SERVER_A)).isEqualTo(SATISFIED);
    }

    @Test
    void shouldKeepNearestCursorAfterRoleChange() {
        var monitor = monitor(ReadPreference.nearest());
        var cluster = replicaSet(
                server(SERVER_A, ServerType.REPLICA_SET_PRIMARY),
                server(SERVER_B, ServerType.REPLICA_SET_SECONDARY));

        assertThat(monitor.evaluate(cluster, SERVER_A)).isEqualTo(SATISFIED);
        assertThat(monitor.evaluate(cluster, SERVER_B)).isEqualTo(SATISFIED);
    }

    @Test
    void shouldReportWhenExactPreferenceHasNoEligibleServer() {
        var primary = monitor(ReadPreference.primary());
        var secondary = monitor(ReadPreference.secondary());
        var onlyPrimary = replicaSet(server(SERVER_A, ServerType.REPLICA_SET_PRIMARY));
        var onlySecondary = replicaSet(server(SERVER_B, ServerType.REPLICA_SET_SECONDARY));

        assertThat(primary.evaluate(onlySecondary, SERVER_B)).isEqualTo(NO_ELIGIBLE_SERVER);
        assertThat(secondary.evaluate(onlyPrimary, SERVER_A)).isEqualTo(NO_ELIGIBLE_SERVER);
        assertThat(primary.shouldWaitForEligibleServer(onlySecondary)).isTrue();
        assertThat(secondary.shouldWaitForEligibleServer(onlyPrimary)).isTrue();
        assertThat(primary.shouldWaitForEligibleServer(onlyPrimary)).isFalse();
        assertThat(secondary.shouldWaitForEligibleServer(onlySecondary)).isFalse();

        var unknown = new ClusterDescription(
                ClusterConnectionMode.MULTIPLE,
                ClusterType.UNKNOWN,
                List.of());
        assertThat(primary.shouldWaitForEligibleServer(unknown)).isTrue();
    }

    @Test
    void shouldReportWhenCursorServerCannotBeVerified() {
        var monitor = monitor(ReadPreference.primary());
        var cluster = replicaSet(server(SERVER_A, ServerType.REPLICA_SET_PRIMARY));

        assertThat(monitor.evaluate(cluster, SERVER_B)).isEqualTo(UNVERIFIED);
    }

    @Test
    void shouldIgnoreUnsupportedTopologies() {
        var monitor = monitor(ReadPreference.primary());
        var primary = server(SERVER_A, ServerType.REPLICA_SET_PRIMARY);
        var sharded = new ClusterDescription(
                ClusterConnectionMode.MULTIPLE,
                ClusterType.SHARDED,
                List.of(server(SERVER_A, ServerType.SHARD_ROUTER)));
        var direct = new ClusterDescription(
                ClusterConnectionMode.SINGLE,
                ClusterType.REPLICA_SET,
                List.of(primary));

        assertThat(monitor.evaluate(sharded, SERVER_A)).isEqualTo(SATISFIED);
        assertThat(monitor.evaluate(direct, SERVER_A)).isEqualTo(SATISFIED);
        assertThat(monitor.shouldWaitForEligibleServer(sharded)).isFalse();
        assertThat(monitor.shouldWaitForEligibleServer(direct)).isFalse();
    }

    @Test
    void shouldCheckAtHeartbeatInterval() {
        var time = new AtomicLong(100);
        var monitor = new MongoDbReadPreferenceMonitor(ReadPreference.primary(), 10, time::get);
        var cluster = replicaSet(
                server(SERVER_A, ServerType.REPLICA_SET_SECONDARY),
                server(SERVER_B, ServerType.REPLICA_SET_PRIMARY));

        assertThat(monitor.isCheckDue()).isFalse();
        time.set(109);
        assertThat(monitor.isCheckDue()).isFalse();
        time.set(110);
        assertThat(monitor.isCheckDue()).isTrue();
        assertThat(monitor.evaluate(cluster, SERVER_A)).isEqualTo(RELOCATE);
        assertThat(monitor.isCheckDue()).isFalse();
    }

    @Test
    void shouldUseMonotonicTimeForCheckInterval() {
        var wallTime = new AtomicLong(100);
        var monotonicTime = new AtomicLong(100);
        var clock = new Clock() {
            @Override
            public long currentTimeInMillis() {
                return wallTime.get();
            }

            @Override
            public long currentTimeInNanos() {
                return TimeUnit.MILLISECONDS.toNanos(monotonicTime.get());
            }
        };
        var monitor = new MongoDbReadPreferenceMonitor(ReadPreference.primary(), 10, clock);
        var cluster = replicaSet(
                server(SERVER_A, ServerType.REPLICA_SET_SECONDARY),
                server(SERVER_B, ServerType.REPLICA_SET_PRIMARY));

        wallTime.set(50);
        monotonicTime.set(109);
        assertThat(monitor.isCheckDue()).isFalse();
        monotonicTime.set(110);
        assertThat(monitor.isCheckDue()).isTrue();
        assertThat(monitor.evaluate(cluster, SERVER_A)).isEqualTo(RELOCATE);
    }

    private static MongoDbReadPreferenceMonitor monitor(ReadPreference readPreference) {
        return new MongoDbReadPreferenceMonitor(readPreference, 1, () -> 0);
    }

    private static ClusterDescription replicaSet(ServerDescription... servers) {
        return new ClusterDescription(
                ClusterConnectionMode.MULTIPLE,
                ClusterType.REPLICA_SET,
                List.of(servers));
    }

    private static ServerDescription server(ServerAddress address, ServerType type) {
        return ServerDescription.builder()
                .address(address)
                .type(type)
                .state(ServerConnectionState.CONNECTED)
                .ok(true)
                .setName("rs0")
                .roundTripTime(1, TimeUnit.MILLISECONDS)
                .lastWriteDate(new Date())
                .build();
    }
}
