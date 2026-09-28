/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection.client;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.bson.BsonDocument;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.internal.operation.ListCollectionsOperation;
import com.mongodb.internal.operation.ListDatabasesOperation;

final class MongoDbClientOperations {

    static void forEachDatabaseName(MongoClient client, Supplier<MongoClient> fallbackClientSupplier, Consumer<String> operation) {
        executeWithNativeClient(client, fallbackClientSupplier,
                mongoClient -> forEachDatabaseName(mongoClient, client.getReadPreference(), operation));
    }

    static void forEachCollectionNameInDatabase(MongoClient client, Supplier<MongoClient> fallbackClientSupplier,
                                                String databaseName, Consumer<String> operation) {
        executeWithNativeClient(client, fallbackClientSupplier,
                mongoClient -> forEachCollectionNameInDatabase(mongoClient, client.getReadPreference(), databaseName, operation));
    }

    private static void forEachDatabaseName(MongoClientImpl client, ReadPreference readPreference, Consumer<String> operation) {
        var listDatabases = new ListDatabasesOperation<>(
                client.getCodecRegistry().get(BsonDocument.class))
                .nameOnly(true)
                .retryReads(client.getSettings().getRetryReads());

        try (var cursor = client.getOperationExecutor().execute(
                listDatabases,
                readPreference,
                ReadConcern.DEFAULT)) {
            while (cursor.hasNext()) {
                cursor.next().stream()
                        .map(database -> database.getString("name").getValue())
                        .forEach(operation);
            }
        }
    }

    private static void forEachCollectionNameInDatabase(MongoClientImpl client, ReadPreference readPreference,
                                                        String databaseName, Consumer<String> operation) {
        var listCollections = new ListCollectionsOperation<>(
                databaseName,
                client.getCodecRegistry().get(BsonDocument.class))
                .nameOnly(true)
                .retryReads(client.getSettings().getRetryReads());

        try (var cursor = client.getOperationExecutor().execute(
                listCollections,
                readPreference,
                ReadConcern.DEFAULT)) {
            while (cursor.hasNext()) {
                cursor.next().stream()
                        .map(collection -> collection.getString("name").getValue())
                        .forEach(operation);
            }
        }
    }

    private static void executeWithNativeClient(MongoClient client, Supplier<MongoClient> fallbackClientSupplier,
                                                Consumer<MongoClientImpl> operation) {
        if (client instanceof MongoClientImpl mongoClient) {
            operation.accept(mongoClient);
            return;
        }

        try (var fallbackClient = fallbackClientSupplier.get()) {
            if (!(fallbackClient instanceof MongoClientImpl mongoClient)) {
                throw new IllegalArgumentException("Metadata MongoClient must be a driver-native client");
            }
            operation.accept(mongoClient);
        }
    }

    private MongoDbClientOperations() {
    }
}
