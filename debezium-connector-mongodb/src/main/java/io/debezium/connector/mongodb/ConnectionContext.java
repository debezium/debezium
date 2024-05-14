/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.ClusterDescription;

import io.debezium.config.Configuration;
import io.debezium.function.BlockingConsumer;
import io.debezium.util.Clock;
import io.debezium.util.DelayStrategy;
import io.debezium.util.Metronome;

/**
 * @author Randall Hauch
 *
 */
public class ConnectionContext implements AutoCloseable {

    /**
     * A pause between failed MongoDB operations to prevent CPU throttling and DoS of
     * target MongoDB database.
     */
    private static final Duration PAUSE_AFTER_ERROR = Duration.ofMillis(500);

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionContext.class);

    protected final Configuration config;
    protected final MongoClients pool;
    protected final ReplicaSets replicaSets;
    protected final DelayStrategy primaryBackoffStrategy;
    protected final boolean useHostsAsSeeds;

    /**
     * @param config the configuration
     */
    public ConnectionContext(Configuration config) {
        this.config = config;

        this.useHostsAsSeeds = config.getBoolean(MongoDbConnectorConfig.AUTO_DISCOVER_MEMBERS);
        final String username = config.getString(MongoDbConnectorConfig.USER);
        final String password = config.getString(MongoDbConnectorConfig.PASSWORD);
        final String adminDbName = config.getString(MongoDbConnectorConfig.AUTH_SOURCE);
        final boolean useSSL = config.getBoolean(MongoDbConnectorConfig.SSL_ENABLED);
        final boolean sslAllowInvalidHostnames = config.getBoolean(MongoDbConnectorConfig.SSL_ALLOW_INVALID_HOSTNAMES);

        final int connectTimeoutMs = config.getInteger(MongoDbConnectorConfig.CONNECT_TIMEOUT_MS);
        final int heartbeatFrequencyMs = config.getInteger(MongoDbConnectorConfig.HEARTBEAT_FREQUENCY_MS);
        final int socketTimeoutMs = config.getInteger(MongoDbConnectorConfig.SOCKET_TIMEOUT_MS);
        final int serverSelectionTimeoutMs = config.getInteger(MongoDbConnectorConfig.SERVER_SELECTION_TIMEOUT_MS);

        // Set up the client pool so that it ...
        MongoClients.Builder clientBuilder = MongoClients.create();
        if (username != null || password != null) {
            clientBuilder.withCredential(MongoCredential.createCredential(username, adminDbName, password.toCharArray()));
        }
        if (useSSL) {
            clientBuilder.settings().applyToSslSettings(
                    builder -> builder.enabled(true).invalidHostNameAllowed(sslAllowInvalidHostnames));
        }

        clientBuilder.settings()
                .applyToSocketSettings(builder -> builder.connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                        .readTimeout(socketTimeoutMs, TimeUnit.MILLISECONDS))
                .applyToClusterSettings(
                        builder -> builder.serverSelectionTimeout(serverSelectionTimeoutMs, TimeUnit.MILLISECONDS))
                .applyToServerSettings(
                        builder -> builder.heartbeatFrequency(heartbeatFrequencyMs, TimeUnit.MILLISECONDS))
                .applicationName("debezium.1.9");

        pool = clientBuilder.build();

        this.replicaSets = ReplicaSets.parse(hosts());

        final int initialDelayInMs = config.getInteger(MongoDbConnectorConfig.CONNECT_BACKOFF_INITIAL_DELAY_MS);
        final long maxDelayInMs = config.getLong(MongoDbConnectorConfig.CONNECT_BACKOFF_MAX_DELAY_MS);
        this.primaryBackoffStrategy = DelayStrategy.exponential(initialDelayInMs, maxDelayInMs);
    }

    public void shutdown() {
        try {
            // Closing all connections ...
            logger().info("Closing all connections to {}", replicaSets);
            pool.clear();
        }
        catch (Throwable e) {
            logger().error("Unexpected error shutting down the MongoDB clients", e);
        }
    }

    @Override
    public final void close() {
        shutdown();
    }

    protected Logger logger() {
        return LOGGER;
    }

    public MongoClients clients() {
        return pool;
    }

    public ReplicaSets replicaSets() {
        return replicaSets;
    }

    public boolean performSnapshotEvenIfNotNeeded() {
        return false;
    }

    public MongoClient clientForReplicaSet(ReplicaSet replicaSet) {
        return clientFor(replicaSet.addresses());
    }

    public MongoClient clientFor(String seedAddresses) {
        List<ServerAddress> addresses = MongoUtil.parseAddresses(seedAddresses);
        return clientFor(addresses);
    }

    public MongoClient clientFor(List<ServerAddress> addresses) {
        if (this.useHostsAsSeeds || addresses.isEmpty()) {
            return pool.clientForMembers(addresses);
        }
        return pool.clientFor(addresses.get(0));
    }

    public String hosts() {
        return config.getString(MongoDbConnectorConfig.HOSTS);
    }

    public Duration pollInterval() {
        if (config.hasKey(MongoDbConnectorConfig.MONGODB_POLL_INTERVAL_MS.name())) {
            return Duration.ofMillis(config.getLong(MongoDbConnectorConfig.MONGODB_POLL_INTERVAL_MS));
        }
        if (config.hasKey(MongoDbConnectorConfig.POLL_INTERVAL_SEC.name())) {
            LOGGER.warn("The option `mongodb.poll.interval.sec` is deprecated. Use `mongodb.poll.interval.ms` instead.");
        }
        return Duration.ofSeconds(config.getInteger(MongoDbConnectorConfig.POLL_INTERVAL_SEC));
    }

    public int maxConnectionAttemptsForPrimary() {
        return config.getInteger(MongoDbConnectorConfig.MAX_FAILED_CONNECTIONS);
    }

    /**
     * Obtain a client that will repeated try to obtain a client to the primary node of the replica set, waiting (and using
     * this context's back-off strategy) if required until the primary becomes available.
     *
     * @param replicaSet the replica set information; may not be null
     * @param filters the filter configuration
     * @param errorHandler the function to be called whenever the primary is unable to
     *            {@link MongoPrimary#execute(String, Consumer) execute} an operation to completion; may be null
     * @return the client, or {@code null} if no primary could be found for the replica set
     */
    public ConnectionContext.MongoPrimary primaryFor(ReplicaSet replicaSet, Filters filters, BiConsumer<String, Throwable> errorHandler) {
        return new ConnectionContext.MongoPrimary(this, replicaSet, filters, errorHandler);
    }

    /**
     * Obtain a client that will repeated try to obtain a client to the primary node of the replica set, waiting (and using
     * this context's back-off strategy) if required until the primary becomes available.
     *
     * @param replicaSet the replica set information; may not be null
     * @return the client, or {@code null} if no primary could be found for the replica set
     */
    protected Supplier<MongoClient> primaryClientFor(ReplicaSet replicaSet) {
        return primaryClientFor(replicaSet, (attempts, remaining, error) -> {
            if (error == null) {
                logger().info("Unable to connect to primary node of '{}' after attempt #{} ({} remaining)", replicaSet, attempts, remaining);
            }
            else {
                logger().error("Error while attempting to connect to primary node of '{}' after attempt #{} ({} remaining): {}", replicaSet,
                        attempts, remaining, error.getMessage(), error);
            }
        });
    }

    /**
     * Obtain a client that will repeated try to obtain a client to the primary node of the replica set, waiting (and using
     * this context's back-off strategy) if required until the primary becomes available.
     *
     * @param replicaSet the replica set information; may not be null
     * @param handler the function that will be called when the primary could not be obtained; may not be null
     * @return the client, or {@code null} if no primary could be found for the replica set
     */
    protected Supplier<MongoClient> primaryClientFor(ReplicaSet replicaSet, PrimaryConnectFailed handler) {
        Supplier<MongoClient> factory = () -> clientForPrimary(replicaSet);
        int maxAttempts = maxConnectionAttemptsForPrimary();
        return () -> {
            int attempts = 0;
            MongoClient primary = null;
            while (primary == null) {
                ++attempts;
                try {
                    // Try to get the primary
                    primary = factory.get();
                    if (primary != null) {
                        break;
                    }
                }
                catch (Throwable t) {
                    handler.failed(attempts, maxAttempts - attempts, t);
                }
                if (attempts > maxAttempts) {
                    throw new ConnectException("Unable to connect to primary node of '" + replicaSet + "' after " +
                            attempts + " failed attempts");
                }
                handler.failed(attempts, maxAttempts - attempts, null);
                primaryBackoffStrategy.sleepWhen(true);
                continue;
            }
            return primary;
        };
    }

    @FunctionalInterface
    public static interface PrimaryConnectFailed {
        void failed(int attemptNumber, int attemptsRemaining, Throwable error);
    }

    /**
     * A supplier of a client that connects only to the primary of a replica set. Operations on the primary will continue
     */
    public static class MongoPrimary {
        private final ReplicaSet replicaSet;
        private final Supplier<MongoClient> primaryConnectionSupplier;
        private final Filters filters;
        private final BiConsumer<String, Throwable> errorHandler;
        private final AtomicBoolean running = new AtomicBoolean(true);

        protected MongoPrimary(ConnectionContext context, ReplicaSet replicaSet, Filters filters, BiConsumer<String, Throwable> errorHandler) {
            this.replicaSet = replicaSet;
            this.primaryConnectionSupplier = context.primaryClientFor(replicaSet);
            this.filters = filters;
            this.errorHandler = errorHandler;
        }

        /**
         * Get the replica set.
         *
         * @return the replica set; never null
         */
        public ReplicaSet replicaSet() {
            return replicaSet;
        }

        /**
         * Get the address of the primary node, if there is one.
         *
         * @return the address of the replica set's primary node, or {@code null} if there is currently no primary
         */
        public ServerAddress address() {
            return execute("get replica set primary", primary -> {
                return MongoUtil.getPrimaryAddress(primary);
            });
        }

        /**
         * Execute the supplied operation using the primary, blocking until a primary is available. Whenever the operation stops
         * (e.g., if the primary is no longer primary), then restart the operation using the current primary.
         *
         * @param desc the description of the operation, for logging purposes
         * @param operation the operation to be performed on the primary.
         */
        public void execute(String desc, Consumer<MongoClient> operation) {
            final Metronome errorMetronome = Metronome.sleeper(PAUSE_AFTER_ERROR, Clock.SYSTEM);
            while (true) {
                MongoClient primary = primaryConnectionSupplier.get();
                try {
                    operation.accept(primary);
                    return;
                }
                catch (Throwable t) {
                    errorHandler.accept(desc, t);
                    if (!isRunning()) {
                        throw new ConnectException("Operation failed and MongoDB primary termination requested", t);
                    }
                    try {
                        errorMetronome.pause();
                    }
                    catch (InterruptedException e) {
                        // Interruption is not propagated
                    }
                }
            }
        }

        /**
         * Execute the supplied operation using the primary, blocking until a primary is available. Whenever the operation stops
         * (e.g., if the primary is no longer primary), then restart the operation using the current primary.
         *
         * @param desc the description of the operation, for logging purposes
         * @param operation the operation to be performed on the primary
         * @return return value of the executed operation
         */
        public <T> T execute(String desc, Function<MongoClient, T> operation) {
            final Metronome errorMetronome = Metronome.sleeper(PAUSE_AFTER_ERROR, Clock.SYSTEM);
            while (true) {
                MongoClient primary = primaryConnectionSupplier.get();
                try {
                    return operation.apply(primary);
                }
                catch (Throwable t) {
                    errorHandler.accept(desc, t);
                    if (!isRunning()) {
                        throw new ConnectException("Operation failed and MongoDB primary termination requested", t);
                    }
                    try {
                        errorMetronome.pause();
                    }
                    catch (InterruptedException e) {
                        // Interruption is not propagated
                    }
                }
            }
        }

        /**
         * Execute the supplied operation using the primary, blocking until a primary is available. Whenever the operation stops
         * (e.g., if the primary is no longer primary), then restart the operation using the current primary.
         *
         * @param desc the description of the operation, for logging purposes
         * @param operation the operation to be performed on the primary.
         * @throws InterruptedException if the operation was interrupted
         */
        public void executeBlocking(String desc, BlockingConsumer<MongoClient> operation) throws InterruptedException {
            final Metronome errorMetronome = Metronome.sleeper(PAUSE_AFTER_ERROR, Clock.SYSTEM);
            while (true) {
                MongoClient primary = primaryConnectionSupplier.get();
                try {
                    operation.accept(primary);
                    return;
                }
                catch (InterruptedException e) {
                    throw e;
                }
                catch (Throwable t) {
                    errorHandler.accept(desc, t);
                    if (!isRunning()) {
                        throw new ConnectException("Operation failed and MongoDB primary termination requested", t);
                    }
                    errorMetronome.pause();
                }
            }
        }

        /**
         * Use the primary to get the names of all the databases in the replica set, applying the current database
         * filter configuration. This method will block until a primary can be obtained to get the names of all
         * databases in the replica set.
         *
         * @return the database names; never null but possibly empty
         */
        public Set<String> databaseNames() {
            return execute("get database names", primary -> {
                Set<String> databaseNames = new HashSet<>();

                MongoUtil.forEachDatabaseName(
                        primary,
                        dbName -> {
                            if (filters.databaseFilter().test(dbName)) {
                                databaseNames.add(dbName);
                            }
                        });

                return databaseNames;
            });
        }

        /**
         * Use the primary to get the identifiers of all the collections in the replica set, applying the current
         * collection filter configuration. This method will block until a primary can be obtained to get the
         * identifiers of all collections in the replica set.
         *
         * @return the collection identifiers; never null
         */
        public List<CollectionId> collections() {
            String replicaSetName = replicaSet.replicaSetName();

            // For each database, get the list of collections ...
            return execute("get collections in databases", primary -> {
                List<CollectionId> collections = new ArrayList<>();
                Set<String> databaseNames = databaseNames();

                for (String dbName : databaseNames) {
                    MongoUtil.forEachCollectionNameInDatabase(primary, dbName, collectionName -> {
                        CollectionId collectionId = new CollectionId(replicaSetName, dbName, collectionName);

                        if (filters.collectionFilter().test(collectionId)) {
                            collections.add(collectionId);
                        }
                    });
                }

                return collections;
            });
        }

        private boolean isRunning() {
            return running.get();
        }

        /**
         * Terminates the execution loop of the current primary
         */
        public void stop() {
            running.set(false);
        }
    }

    /**
     * Obtain a client that talks only to the primary node of the replica set.
     *
     * @param replicaSet the replica set information; may not be null
     * @return the client, or {@code null} if no primary could be found for the replica set
     */
    protected MongoClient clientForPrimary(ReplicaSet replicaSet) {
        MongoClient replicaSetClient = clientForReplicaSet(replicaSet);
        final ClusterDescription clusterDescription = replicaSetClient.getClusterDescription();
        if (clusterDescription == null) {
            if (!this.useHostsAsSeeds) {
                // No replica set status is available, but it may still be a replica set ...
                return replicaSetClient;
            }
            // This is not a replica set, so there will be no oplog to read ...
            throw new ConnectException("The MongoDB server(s) at '" + replicaSet +
                    "' is not a valid replica set and cannot be used");
        }
        // It is a replica set ...
        ServerAddress primaryAddress = MongoUtil.getPrimaryAddress(replicaSetClient);
        if (primaryAddress != null) {
            return pool.clientFor(primaryAddress);
        }
        return null;
    }
}
