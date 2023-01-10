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
import java.util.Optional;
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

import com.mongodb.ConnectionString;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;

import io.debezium.config.Configuration;
import io.debezium.function.BlockingConsumer;
import io.debezium.util.Clock;
import io.debezium.util.DelayStrategy;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;

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
    protected final DelayStrategy backoffStrategy;
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

        clientBuilder.settings()
                .applyToSocketSettings(builder -> builder.connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                        .readTimeout(socketTimeoutMs, TimeUnit.MILLISECONDS))
                .applyToClusterSettings(
                        builder -> builder.serverSelectionTimeout(serverSelectionTimeoutMs, TimeUnit.MILLISECONDS))
                .applyToServerSettings(
                        builder -> builder.heartbeatFrequency(heartbeatFrequencyMs, TimeUnit.MILLISECONDS));

        // Use credentials if provided as part of connection String
        var cs = connectionString();
        cs
                .map(ConnectionString::getCredential)
                .ifPresent(clientBuilder::withCredential);

        // Use credential if provided as properties
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
                        builder -> builder.serverSelectionTimeout(serverSelectionTimeoutMs, TimeUnit.MILLISECONDS));

        pool = clientBuilder.build();

        final int initialDelayInMs = config.getInteger(MongoDbConnectorConfig.CONNECT_BACKOFF_INITIAL_DELAY_MS);
        final long maxDelayInMs = config.getLong(MongoDbConnectorConfig.CONNECT_BACKOFF_MAX_DELAY_MS);
        this.backoffStrategy = DelayStrategy.exponential(Duration.ofMillis(initialDelayInMs), Duration.ofMillis(maxDelayInMs));
    }

    public void shutdown() {
        try {
            // Closing all connections ...
            logger().info("Closing all connections to {}", connectionSeed());
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

    public boolean performSnapshotEvenIfNotNeeded() {
        return false;
    }

    public MongoClient clientFor(ReplicaSet replicaSet) {
        return clientFor(replicaSet.addresses());
    }

    public MongoClient clientFor(String seedAddresses) {
        List<ServerAddress> addresses = MongoUtil.parseAddresses(seedAddresses);
        return clientFor(addresses);
    }

    public MongoClient clientForSeedConnection() {
        return connectionString()
                .map(this::clientFor)
                .orElseGet(() -> clientFor(hosts()));
    }

    public MongoClient clientFor(ConnectionString connectionString) {
        return pool.clientForMembers(connectionString);
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

    /**
     * Initial connection seed which is either a host specification or connection string
     *
     * @return hosts or connection string
     */
    public String connectionSeed() {
        return connectionString()
                .map(ConnectionString::toString)
                .orElse(config.getString(MongoDbConnectorConfig.HOSTS));
    }

    /**
     * Same as {@link #connectionSeed()} but masks sensitive information
     *
     * @return masked connection seed
     */
    public String maskedConnectionSeed() {
        var connectionSeed = connectionSeed();

        return connectionString()
                .map(ConnectionString::getCredential)
                .map(creds -> Strings.mask(
                        connectionSeed,
                        creds.getUserName(),
                        creds.getSource(),
                        creds.getPassword() != null ? String.valueOf(creds.getPassword()) : null))
                .orElse(connectionSeed);
    }

    public Optional<ConnectionString> connectionString() {
        String raw = config.getString(MongoDbConnectorConfig.CONNECTION_STRING);
        return Optional.ofNullable(raw).map(ConnectionString::new);
    }

    public Duration pollInterval() {
        return Duration.ofMillis(config.getLong(MongoDbConnectorConfig.MONGODB_POLL_INTERVAL_MS));
    }

    /**
     * Obtain a client that will repeatedly try to obtain a client to the primary node of the replica set, waiting (and using
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
     * Obtain a client that will repeatedly try to obtain a client to a node of preferred type of the replica set, waiting (and using
     * this context's back-off strategy) if required until the node becomes available.
     *
     * @param replicaSet the replica set information; may not be null
     * @param filters the filter configuration
     * @param errorHandler the function to be called whenever the node is unable to
     *            {@link MongoPreferredNode#execute(String, Consumer) execute} an operation to completion; may be null
     * @return the client, or {@code null} if no primary could be found for the replica set
     */
    public ConnectionContext.MongoPreferredNode preferredFor(ReplicaSet replicaSet, ReadPreference preference, Filters filters,
                                                             BiConsumer<String, Throwable> errorHandler) {
        return new ConnectionContext.MongoPreferredNode(this, replicaSet, preference, filters, errorHandler);
    }

    /**
     * Obtain a client that will repeatedly try to obtain a client to a node of preferred type of the replica set, waiting (and using
     * this context's back-off strategy) if required until the node becomes available.
     *
     * @param replicaSet the replica set information; may not be null
     * @return the client, or {@code null} if no node of preferred type could be found for the replica set
     */
    protected Supplier<MongoClient> preferredClientFor(ReplicaSet replicaSet, ReadPreference preference) {
        return preferredClientFor(replicaSet, preference, (attempts, remaining, error) -> {
            if (error == null) {
                logger().info("Unable to connect to {} node of '{}' after attempt #{} ({} remaining)", preference.getName(),
                        replicaSet, attempts, remaining);
            }
            else {
                logger().error("Error while attempting to connect to {} node of '{}' after attempt #{} ({} remaining): {}", preference.getName(),
                        replicaSet, attempts, remaining, error.getMessage(), error);
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
    protected Supplier<MongoClient> preferredClientFor(ReplicaSet replicaSet, ReadPreference preference, PreferredConnectFailed handler) {
        Supplier<MongoClient> factory = () -> clientForPreferred(replicaSet, preference);
        int maxAttempts = config.getInteger(MongoDbConnectorConfig.MAX_FAILED_CONNECTIONS);
        return () -> {
            int attempts = 0;
            MongoClient client = null;
            while (client == null) {
                ++attempts;
                try {
                    // Try to get the primary
                    client = factory.get();
                    if (client != null) {
                        break;
                    }
                }
                catch (Throwable t) {
                    handler.failed(attempts, maxAttempts - attempts, t);
                }
                if (attempts > maxAttempts) {
                    throw new ConnectException("Unable to connect to " + preference.getName() + " node of '" + replicaSet +
                            "' after " + attempts + " failed attempts");
                }
                handler.failed(attempts, maxAttempts - attempts, null);
                backoffStrategy.sleepWhen(true);
            }
            return client;
        };
    }

    @FunctionalInterface
    public interface PreferredConnectFailed {
        void failed(int attemptNumber, int attemptsRemaining, Throwable error);
    }

    /**
     * A supplier of a client that connects only to the primary of a replica set. Operations on the primary will continue
     */
    public static class MongoPrimary extends MongoPreferredNode {
        protected MongoPrimary(ConnectionContext context, ReplicaSet replicaSet, Filters filters, BiConsumer<String, Throwable> errorHandler) {
            super(context, replicaSet, ReadPreference.primary(), filters, errorHandler);
        }
    }

    /**
     * A supplier of a client that connects only to node of preferred type from a replica set. Operations on this node will continue
     */
    public static class MongoPreferredNode {
        private final ReplicaSet replicaSet;
        private final Supplier<MongoClient> connectionSupplier;
        private final Filters filters;
        private final BiConsumer<String, Throwable> errorHandler;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final ReadPreference preference;

        protected MongoPreferredNode(
                                     ConnectionContext context,
                                     ReplicaSet replicaSet,
                                     ReadPreference preference,
                                     Filters filters,
                                     BiConsumer<String, Throwable> errorHandler) {
            this.replicaSet = replicaSet;
            this.preference = preference;
            this.connectionSupplier = context.preferredClientFor(replicaSet, preference);
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
         * Get read preference of
         *
         * @return the read preference
         */
        public ReadPreference getPreference() {
            return preference;
        }

        /**
         * Get the address of the node with preferred type, if there is one.
         *
         * @return the address of the replica set's preferred node, or {@code null} if there is currently no node of preferred type
         */
        public ServerAddress address() {
            return execute("get replica set " + preference.getName(), client -> {
                return MongoUtil.getPreferredAddress(client, preference);
            });
        }

        /**
         * Execute the supplied operation using the preferred node, blocking until it is available. Whenever the operation stops
         * (e.g., if the node is no longer of preferred type), then restart the operation using a current node of that type.
         *
         * @param desc the description of the operation, for logging purposes
         * @param operation the operation to be performed on a node of preferred type.
         */
        public void execute(String desc, Consumer<MongoClient> operation) {
            final Metronome errorMetronome = Metronome.sleeper(PAUSE_AFTER_ERROR, Clock.SYSTEM);
            while (true) {
                MongoClient client = connectionSupplier.get();
                try {
                    operation.accept(client);
                    return;
                }
                catch (Throwable t) {
                    errorHandler.accept(desc, t);
                    if (!isRunning()) {
                        throw new ConnectException("Operation failed and MongoDB " + preference.getName() + " termination requested", t);
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
         * Execute the supplied operation using the preferred node, blocking until it is available. Whenever the operation stops
         * (e.g., if the node is no longer of preferred type), then restart the operation using a current node of that type.
         *
         * @param desc the description of the operation, for logging purposes
         * @param operation the operation to be performed on a node of preferred type
         * @return return value of the executed operation
         */
        public <T> T execute(String desc, Function<MongoClient, T> operation) {
            final Metronome errorMetronome = Metronome.sleeper(PAUSE_AFTER_ERROR, Clock.SYSTEM);
            while (true) {
                MongoClient client = connectionSupplier.get();
                try {
                    return operation.apply(client);
                }
                catch (Throwable t) {
                    errorHandler.accept(desc, t);
                    if (!isRunning()) {
                        throw new ConnectException("Operation failed and MongoDB " + preference.getName() + " termination requested", t);
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
         * Execute the supplied operation using the preferred node, blocking until it is available. Whenever the operation stops
         * (e.g., if the node is no longer of preferred type), then restart the operation using a current node of that type.
         *
         * @param desc the description of the operation, for logging purposes
         * @param operation the operation to be performed on a node of preferred type.
         * @throws InterruptedException if the operation was interrupted
         */
        public void executeBlocking(String desc, BlockingConsumer<MongoClient> operation) throws InterruptedException {
            final Metronome errorMetronome = Metronome.sleeper(PAUSE_AFTER_ERROR, Clock.SYSTEM);
            while (true) {
                MongoClient client = connectionSupplier.get();
                try {
                    operation.accept(client);
                    return;
                }
                catch (InterruptedException e) {
                    throw e;
                }
                catch (Throwable t) {
                    errorHandler.accept(desc, t);
                    if (!isRunning()) {
                        throw new ConnectException("Operation failed and MongoDB " + preference.getName() + " termination requested", t);
                    }
                    errorMetronome.pause();
                }
            }
        }

        /**
         * Use a node of preferred type to get the names of all the databases in the replica set, applying the current database
         * filter configuration. This method will block until a node of preferred type can be obtained to get the names of all
         * databases in the replica set.
         *
         * @return the database names; never null but possibly empty
         */
        public Set<String> databaseNames() {
            return execute("get database names", client -> {
                Set<String> databaseNames = new HashSet<>();

                MongoUtil.forEachDatabaseName(
                        client,
                        dbName -> {
                            if (filters.databaseFilter().test(dbName)) {
                                databaseNames.add(dbName);
                            }
                        });

                return databaseNames;
            });
        }

        /**
         * Use a node of preferred type to get the identifiers of all the collections in the replica set, applying the current
         * collection filter configuration. This method will block until a primary can be obtained to get the
         * identifiers of all collections in the replica set.
         *
         * @return the collection identifiers; never null
         */
        public List<CollectionId> collections() {
            String replicaSetName = replicaSet.replicaSetName();

            // For each database, get the list of collections ...
            return execute("get collections in databases", client -> {
                List<CollectionId> collections = new ArrayList<>();
                Set<String> databaseNames = databaseNames();

                for (String dbName : databaseNames) {
                    MongoUtil.forEachCollectionNameInDatabase(client, dbName, collectionName -> {
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
    protected MongoClient clientForPreferred(ReplicaSet replicaSet, ReadPreference preference) {
        MongoClient replicaSetClient = clientFor(replicaSet);
        final ClusterDescription clusterDescription = MongoUtil.clusterDescription(replicaSetClient);
        if (clusterDescription.getType() == ClusterType.UNKNOWN) {
            if (!this.useHostsAsSeeds) {
                // No replica set status is available, but it may still be a replica set ...
                return replicaSetClient;
            }
            // This is not a replica set, so there will be no oplog to read ...
            throw new ConnectException("The MongoDB server(s) at '" + replicaSet +
                    "' is not a valid replica set and cannot be used");
        }
        // It is a replica set ...
        ServerAddress preferredAddress = MongoUtil.getPreferredAddress(replicaSetClient, preference);
        if (preferredAddress != null) {
            return pool.clientFor(preferredAddress);
        }
        return null;
    }
}
