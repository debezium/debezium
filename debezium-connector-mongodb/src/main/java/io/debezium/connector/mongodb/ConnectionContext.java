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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ReplicaSetStatus;
import com.mongodb.ServerAddress;

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

    private final Logger logger = LoggerFactory.getLogger(getClass());
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
        final String adminDbName = ReplicaSetDiscovery.ADMIN_DATABASE_NAME;
        final boolean useSSL = config.getBoolean(MongoDbConnectorConfig.SSL_ENABLED);
        final boolean sslAllowInvalidHostnames = config.getBoolean(MongoDbConnectorConfig.SSL_ALLOW_INVALID_HOSTNAMES);

        // Set up the client pool so that it ...
        MongoClients.Builder clientBuilder = MongoClients.create();
        if (username != null || password != null) {
            clientBuilder.withCredential(MongoCredential.createCredential(username, adminDbName, password.toCharArray()));
        }
        if (useSSL) {
            clientBuilder.options().sslEnabled(true).sslInvalidHostNameAllowed(sslAllowInvalidHostnames);
        }
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
        } catch (Throwable e) {
            logger().error("Unexpected error shutting down the MongoDB clients", e);
        }
    }

    @Override
    public final void close() {
        shutdown();
    }

    protected Logger logger() {
        return logger;
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
        if ( this.useHostsAsSeeds || addresses.isEmpty() ) {
            return pool.clientForMembers(addresses);
        }
        return pool.clientFor(addresses.get(0));
    }

    public String hosts() {
        return config.getString(MongoDbConnectorConfig.HOSTS);
    }

    public int pollPeriodInSeconds() {
        return config.getInteger(MongoDbConnectorConfig.POLL_INTERVAL_SEC);
    }

    public int maxConnectionAttemptsForPrimary() {
        return config.getInteger(MongoDbConnectorConfig.MAX_FAILED_CONNECTIONS);
    }

    public int maxNumberOfCopyThreads() {
        return config.getInteger(MongoDbConnectorConfig.MAX_COPY_THREADS);
    }

    /**
     * Obtain a client that will repeated try to obtain a client to the primary node of the replica set, waiting (and using
     * this context's back-off strategy) if required until the primary becomes available.
     *
     * @param replicaSet the replica set information; may not be null
     * @param errorHandler the function to be called whenever the primary is unable to
     *            {@link MongoPrimary#execute(String, Consumer) execute} an operation to completion; may be null
     * @return the client, or {@code null} if no primary could be found for the replica set
     */
    public ConnectionContext.MongoPrimary primaryFor(ReplicaSet replicaSet, BiConsumer<String, Throwable> errorHandler) {
        return new ConnectionContext.MongoPrimary(this, replicaSet, errorHandler);
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
            } else {
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
                    if (primary != null) break;
                } catch (Throwable t) {
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
        private final BiConsumer<String, Throwable> errorHandler;

        protected MongoPrimary(ConnectionContext context, ReplicaSet replicaSet, BiConsumer<String, Throwable> errorHandler) {
            this.replicaSet = replicaSet;
            this.primaryConnectionSupplier = context.primaryClientFor(replicaSet);
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
            AtomicReference<ServerAddress> address = new AtomicReference<>();
            execute("get replica set primary", primary -> {
                ReplicaSetStatus rsStatus = primary.getReplicaSetStatus();
                if (rsStatus != null) {
                    address.set(rsStatus.getMaster());
                }
            });
            return address.get();
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
                } catch (Throwable t) {
                    errorHandler.accept(desc, t);
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
                } catch (Throwable t) {
                    errorHandler.accept(desc, t);
                    errorMetronome.pause();
                }
            }
        }

        /**
         * Use the primary to get the names of all the databases in the replica set. This method will block until
         * a primary can be obtained to get the names of all databases in the replica set.
         *
         * @return the database names; never null but possibly empty
         */
        public Set<String> databaseNames() {
            Set<String> databaseNames = new HashSet<>();
            execute("get database names", primary -> {
                databaseNames.clear(); // in case we restarted
                MongoUtil.forEachDatabaseName(primary, databaseNames::add);
            });
            return databaseNames;
        }

        /**
         * Use the primary to get the identifiers of all the collections in the replica set. This method will block until
         * a primary can be obtained to get the identifiers of all collections in the replica set.
         *
         * @return the collection identifiers; never null
         */
        public List<CollectionId> collections() {
            String replicaSetName = replicaSet.replicaSetName();
            // For each database, get the list of collections ...
            List<CollectionId> collections = new ArrayList<>();
            execute("get collections in databases", primary -> {
                collections.clear(); // in case we restarted
                Set<String> databaseNames = databaseNames();
                MongoUtil.forEachDatabaseName(primary, databaseNames::add);
                databaseNames.forEach(dbName -> {
                    MongoUtil.forEachCollectionNameInDatabase(primary, dbName, collectionName -> {
                        collections.add(new CollectionId(replicaSetName, dbName, collectionName));
                    });
                });
            });
            return collections;
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
        ReplicaSetStatus rsStatus = replicaSetClient.getReplicaSetStatus();
        if (rsStatus == null) {
            if ( !this.useHostsAsSeeds ) {
                // No replica set status is available, but it may still be a replica set ...
                return replicaSetClient;
            }
            // This is not a replica set, so there will be no oplog to read ...
            throw new ConnectException("The MongoDB server(s) at '" + replicaSet +
                    "' is not a valid replica set and cannot be used");
        }
        // It is a replica set ...
        ServerAddress primaryAddress = rsStatus.getMaster();
        if (primaryAddress != null) {
            return pool.clientFor(primaryAddress);
        }
        return null;
    }
}
