/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.awaitility.Awaitility;

import com.mongodb.MongoClientSettings;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ServerId;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.ConnectionPoolClosedEvent;
import com.mongodb.event.ConnectionPoolCreatedEvent;
import com.mongodb.event.ConnectionPoolListener;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.connection.DefaultMongoDbAuthProvider;

/**
 * Observes real authentication resources and driver lifecycles, scoped to a test configuration.
 */
class ConnectionResourceTracker implements AutoCloseable {
    private static final String TRACKER_ID = "test.connection.tracker.id";
    private static final Map<String, ConnectionResourceTracker> TRACKERS = new ConcurrentHashMap<>();

    private final String id = UUID.randomUUID().toString();
    private final List<TrackingAuthProvider> providers = new CopyOnWriteArrayList<>();
    private final Set<ClusterId> openedClusters = ConcurrentHashMap.newKeySet();
    private final Set<ClusterId> closedClusters = ConcurrentHashMap.newKeySet();
    private final Set<ServerId> openedPools = ConcurrentHashMap.newKeySet();
    private final Set<ServerId> closedPools = ConcurrentHashMap.newKeySet();
    private final Set<Thread> snapshotWorkers = ConcurrentHashMap.newKeySet();
    private final AtomicInteger prematureCloses = new AtomicInteger();

    RuntimeException initFailure;
    RuntimeException settingsFailure;
    RuntimeException closeFailure;
    CommandListener commandListener;

    ConnectionResourceTracker() {
        TRACKERS.put(id, this);
    }

    Configuration track(Configuration config) {
        return config.edit()
                .with(MongoDbConnectorConfig.AUTH_PROVIDER_CLASS, TrackingAuthProvider.class)
                .with(TRACKER_ID, id)
                .build();
    }

    void assertReleased() {
        assertThat(providers).isNotEmpty();
        assertThat(providers).allSatisfy(provider -> {
            assertThat(provider.executor.isShutdown()).isTrue();
            assertThat(provider.closes.get()).isEqualTo(1);
        });
        assertThat(closedPools).containsExactlyInAnyOrderElementsOf(openedPools);
        // Cluster events are asynchronous; pool lifecycle callbacks are synchronous.
        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(openedClusters).containsAll(openedPools.stream().map(ServerId::getClusterId).toList());
            assertThat(closedClusters).containsExactlyInAnyOrderElementsOf(openedClusters);
            assertThat(providers).allSatisfy(provider -> assertThat(provider.executor.isTerminated()).isTrue());
            assertThat(snapshotWorkers).allSatisfy(worker -> assertThat(worker.isAlive()).as(worker.getName()).isFalse());
        });
        assertThat(prematureCloses).hasValue(0);
    }

    void assertClientsCreated() {
        assertThat(openedClusters).isNotEmpty();
        assertThat(openedPools).isNotEmpty();
    }

    void assertSnapshotWorkersCreated() {
        assertThat(snapshotWorkers).isNotEmpty();
    }

    void assertAuthenticationActive() {
        assertThat(providers).isNotEmpty();
        assertThat(providers).allSatisfy(provider -> assertThat(provider.executor.isShutdown()).isFalse());
    }

    void assertClientCount(int expected) {
        assertThat(openedPools.stream().map(ServerId::getClusterId).distinct()).hasSize(expected);
    }

    @Override
    public void close() {
        providers.forEach(provider -> provider.executor.shutdownNow());
        TRACKERS.remove(id);
    }

    public static class TrackingAuthProvider extends DefaultMongoDbAuthProvider implements AutoCloseable {
        private final AtomicInteger closes = new AtomicInteger();
        private final Set<ServerId> activePools = ConcurrentHashMap.newKeySet();
        private ConnectionResourceTracker tracker;
        private ExecutorService executor;

        @Override
        public void init(Configuration config) {
            tracker = TRACKERS.get(config.getString(TRACKER_ID));
            executor = Executors.newSingleThreadExecutor();
            tracker.providers.add(this);
            executor.execute(() -> {
            });
            if (tracker.initFailure != null) {
                throw tracker.initFailure;
            }
            super.init(config);
        }

        @Override
        public MongoClientSettings.Builder addAuthConfig(MongoClientSettings.Builder builder) {
            if (tracker.settingsFailure != null) {
                throw tracker.settingsFailure;
            }
            builder.addCommandListener(new CommandListener() {
                @Override
                public void commandStarted(CommandStartedEvent event) {
                    final var worker = Thread.currentThread();
                    if (worker.getName().contains("-incremental-snapshot-")) {
                        tracker.snapshotWorkers.add(worker);
                    }
                }
            });
            if (tracker.commandListener != null) {
                builder.addCommandListener(tracker.commandListener);
            }
            builder.applyToClusterSettings(cluster -> cluster.addClusterListener(new ClusterListener() {
                @Override
                public void clusterOpening(ClusterOpeningEvent event) {
                    tracker.openedClusters.add(event.getClusterId());
                }

                @Override
                public void clusterClosed(ClusterClosedEvent event) {
                    tracker.closedClusters.add(event.getClusterId());
                }
            }));
            builder.applyToConnectionPoolSettings(pool -> pool.addConnectionPoolListener(new ConnectionPoolListener() {
                @Override
                public void connectionPoolCreated(ConnectionPoolCreatedEvent event) {
                    tracker.openedPools.add(event.getServerId());
                    activePools.add(event.getServerId());
                }

                @Override
                public void connectionPoolClosed(ConnectionPoolClosedEvent event) {
                    tracker.closedPools.add(event.getServerId());
                    activePools.remove(event.getServerId());
                }
            }));
            return super.addAuthConfig(builder);
        }

        @Override
        public void close() {
            closes.incrementAndGet();
            if (!activePools.isEmpty()) {
                tracker.prematureCloses.incrementAndGet();
            }
            executor.shutdownNow();
            if (tracker.closeFailure != null) {
                throw tracker.closeFailure;
            }
        }
    }
}
