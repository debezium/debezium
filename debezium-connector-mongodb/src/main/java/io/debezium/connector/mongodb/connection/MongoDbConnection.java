/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.bson.BsonTimestamp;

import com.mongodb.client.MongoClient;

import io.debezium.DebeziumException;
import io.debezium.connector.mongodb.CollectionId;
import io.debezium.connector.mongodb.Filters;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoDbPartition;
import io.debezium.connector.mongodb.MongoUtil;
import io.debezium.function.BlockingConsumer;
import io.debezium.function.BlockingFunction;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Scoped Mongodb Connection which applies filter configuration and replica set specification when required
 * Internally this wrapper attempts to obtain regular {@link MongoClient} instance
 */
public final class MongoDbConnection implements AutoCloseable {

    public static final String AUTHORIZATION_FAILURE_MESSAGE = "Command failed with error 13";

    @FunctionalInterface
    public interface ErrorHandler {
        /**
         *
         * @param desc      the description of the operation, for logging purposes
         * @param error     the error which triggered this call
         */
        void onError(String desc, Throwable error);
    }

    @FunctionalInterface
    public interface ChangeEventSourceConnectionFactory {
        /**
         * Create connection for given replica set and partition
         *
         * @param replicaSet    the replica set information; may not be null
         * @param partition      database partition
         * @return connection based on given parameters
         */
        MongoDbConnection get(ReplicaSet replicaSet, MongoDbPartition partition);
    }

    /**
     * A pause between failed MongoDB operations to prevent CPU throttling and DoS of
     * target MongoDB database.
     */
    private static final Duration PAUSE_AFTER_ERROR = Duration.ofMillis(500);

    private final Filters filters;
    private final ErrorHandler errorHandler;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final String name;
    private final Supplier<MongoClient> connectionSupplier;
    private final MongoDbConnectorConfig config;

    protected MongoDbConnection(ReplicaSet replicaSet,
                                MongoDbClientFactory clientFactory,
                                MongoDbConnectorConfig config,
                                Filters filters,
                                ErrorHandler errorHandler) {
        this.name = replicaSet.replicaSetName();
        this.connectionSupplier = () -> clientFactory.client(replicaSet);
        this.config = config;
        this.filters = filters;
        this.errorHandler = errorHandler;
    }

    /**
     * Execute the supplied operation. Whenever the operation fails the error handler is called and the operation is repeated
     *
     * @param desc      the description of the operation, for logging purposes
     * @param operation the operation to be performed
     */
    public void execute(String desc, BlockingConsumer<MongoClient> operation) throws InterruptedException {
        execute(desc, client -> {
            operation.accept(client);
            return null;
        });
    }

    /**
     * Execute the supplied operation. Whenever the operation fails the error handler is called and the operation is repeated
     *
     * @param desc      the description of the operation, for logging purposes
     * @param operation the operation to be performed
     * @return return value of the executed operation
     */
    public <T> T execute(String desc, BlockingFunction<MongoClient, T> operation) throws InterruptedException {
        final Metronome errorMetronome = Metronome.sleeper(PAUSE_AFTER_ERROR, Clock.SYSTEM);
        while (true) {
            try (var client = connectionSupplier.get()) {
                return operation.apply(client);
            }
            catch (InterruptedException e) {
                throw e;
            }
            catch (Throwable t) {
                errorHandler.onError(desc, t);
                if (!isRunning()) {
                    throw new DebeziumException("Operation failed and MongoDB connection to '" + name + "' termination requested", t);
                }
                errorMetronome.pause();
            }
        }
    }

    /**
     * Get the names of all the databases applying the current database filter configuration.
     *
     * @return the database names; never null but possibly empty
     */
    public Set<String> databaseNames() throws InterruptedException {
        if (config.getCaptureScope() == MongoDbConnectorConfig.CaptureScope.DATABASE) {
            return config.getCaptureTarget()
                    .filter(dbName -> filters.databaseFilter().test(dbName))
                    .map(Set::of)
                    .orElse(Set.of());
        }

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
     * Get the identifiers of all the collections, applying the current collection filter configuration.
     *
     * @return the collection identifiers; never null
     */
    public List<CollectionId> collections() throws InterruptedException {
        return execute("get collections in databases", client -> {
            List<CollectionId> collections = new ArrayList<>();
            Set<String> databaseNames = databaseNames();

            for (String dbName : databaseNames) {
                MongoUtil.forEachCollectionNameInDatabase(client, dbName, collectionName -> {
                    CollectionId collectionId = new CollectionId(name, dbName, collectionName);

                    if (filters.collectionFilter().test(collectionId)) {
                        collections.add(collectionId);
                    }
                });
            }

            return collections;
        });
    }

    /**
     * Executes the ping command (<a href="https://www.mongodb.com/docs/manual/reference/command/ping/">Ping</a>) using
     * the first available database
     *
     * @return timestamp of the executed operation
     */
    public BsonTimestamp hello() throws InterruptedException {
        return execute("ping on first available database", client -> {
            var dbName = databaseNames().stream().findFirst().orElse("admin");
            return MongoUtil.hello(client, dbName);
        });
    }

    private boolean isRunning() {
        return running.get();
    }

    @Override
    public void close() {
        running.set(false);
    }
}
