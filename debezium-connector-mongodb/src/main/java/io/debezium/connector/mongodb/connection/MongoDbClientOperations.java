/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection;

import java.util.function.Consumer;

import org.bson.BsonDocument;

import com.mongodb.ReadConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.internal.operation.ListCollectionsOperation;
import com.mongodb.internal.operation.ListDatabasesOperation;

/**
 * Executes the driver's list-databases and list-collections operations without their public APIs' primary read preference.
 */
final class MongoDbClientOperations {

    static void forEachDatabaseName(MongoClient client, Consumer<String> operation) {
        var nativeClient = nativeClient(client);
        var listDatabases = new ListDatabasesOperation<>(
                nativeClient.getCodecRegistry().get(BsonDocument.class))
                .nameOnly(true)
                .retryReads(nativeClient.getSettings().getRetryReads());

        try (var cursor = nativeClient.getOperationExecutor().execute(
                listDatabases,
                client.getReadPreference(),
                ReadConcern.DEFAULT)) {
            while (cursor.hasNext()) {
                cursor.next().stream()
                        .map(database -> database.getString("name").getValue())
                        .forEach(operation);
            }
        }
    }

    static void forEachCollectionNameInDatabase(MongoClient client, String databaseName, Consumer<String> operation) {
        var nativeClient = nativeClient(client);
        var listCollections = new ListCollectionsOperation<>(
                databaseName,
                nativeClient.getCodecRegistry().get(BsonDocument.class))
                .nameOnly(true)
                .retryReads(nativeClient.getSettings().getRetryReads());

        try (var cursor = nativeClient.getOperationExecutor().execute(
                listCollections,
                client.getReadPreference(),
                ReadConcern.DEFAULT)) {
            while (cursor.hasNext()) {
                cursor.next().stream()
                        .map(collection -> collection.getString("name").getValue())
                        .forEach(operation);
            }
        }
    }

    private static MongoClientImpl nativeClient(MongoClient client) {
        if (client instanceof MongoClientImpl nativeClient) {
            return nativeClient;
        }
        throw new IllegalArgumentException("MongoDB connector metadata operations require a driver-native MongoClient");
    }

    private MongoDbClientOperations() {
    }
}
