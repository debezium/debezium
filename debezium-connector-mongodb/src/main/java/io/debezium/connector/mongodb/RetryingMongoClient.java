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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;

import io.debezium.DebeziumException;
import io.debezium.function.BlockingConsumer;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Client scoped to specified replica set.
 * Internally this wrapper attempts to obtain regular {@link MongoClient} instance using given back-off strategy
 */
public class RetryingMongoClient {
    /**
     * A pause between failed MongoDB operations to prevent CPU throttling and DoS of
     * target MongoDB database.
     */
    private static final Duration PAUSE_AFTER_ERROR = Duration.ofMillis(500);

    private final Filters filters;
    private final BiConsumer<String, Throwable> errorHandler;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final String replicaSetName;
    private final Supplier<MongoClient> connectionSupplier;

    protected RetryingMongoClient(ReplicaSet replicaSet,
                                  ReadPreference readPreference,
                                  BiFunction<ReplicaSet, ReadPreference, MongoClient> connectionSupplier,
                                  Filters filters,
                                  BiConsumer<String, Throwable> errorHandler) {
        this.replicaSetName = replicaSet.replicaSetName();
        this.connectionSupplier = () -> connectionSupplier.apply(replicaSet, readPreference);
        this.filters = filters;
        this.errorHandler = errorHandler;
    }

    /**
     * Execute the supplied operation using the preferred node, blocking until it is available. Whenever the operation stops
     * (e.g., if the node is no longer of preferred type), then restart the operation using a current node of that type.
     *
     * @param desc      the description of the operation, for logging purposes
     * @param operation the operation to be performed on a node of preferred type.
     */
    public void execute(String desc, Consumer<MongoClient> operation) {
        execute(desc, client -> {
            operation.accept(client);
            return null;
        });
    }

    /**
     * Execute the supplied operation using the preferred node, blocking until it is available. Whenever the operation stops
     * (e.g., if the node is no longer of preferred type), then restart the operation using a current node of that type.
     *
     * @param desc      the description of the operation, for logging purposes
     * @param operation the operation to be performed on a node of preferred type
     * @return return value of the executed operation
     */
    public <T> T execute(String desc, Function<MongoClient, T> operation) {
        final Metronome errorMetronome = Metronome.sleeper(PAUSE_AFTER_ERROR, Clock.SYSTEM);
        MongoClient client = connectionSupplier.get();
        while (true) {
            try {
                return operation.apply(client);
            }
            catch (Throwable t) {
                errorHandler.accept(desc, t);
                if (!isRunning()) {
                    throw new DebeziumException("Operation failed and MongoDB connection '" + replicaSetName + "' termination requested", t);
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
     * @param desc      the description of the operation, for logging purposes
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
                    throw new DebeziumException("Operation failed and MongoDB connection '" + replicaSetName + "' termination requested", t);
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
