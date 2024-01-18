/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.snapshot;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import io.debezium.connector.mongodb.CollectionId;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.pipeline.signal.actions.snapshotting.CloseIncrementalSnapshotWindow;
import io.debezium.pipeline.source.snapshot.incremental.WatermarkWindowCloser;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;

public class MongoDbInsertWindowCloser implements WatermarkWindowCloser {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbInsertWindowCloser.class);
    private static final String DOCUMENT_ID = "_id";

    private final MongoDbConnection mongoDbConnection;
    private final CollectionId collectionId;

    public MongoDbInsertWindowCloser(MongoDbConnection jdbcConnection, CollectionId collectionId) {
        this.mongoDbConnection = jdbcConnection;
        this.collectionId = collectionId;
    }

    @Override
    public void closeWindow(Partition partition, OffsetContext offsetContext, String chunkId) throws InterruptedException {

        mongoDbConnection.execute(
                "emit window close for chunk '" + chunkId + "'",
                client -> {
                    final MongoDatabase database = client.getDatabase(collectionId.dbName());
                    final MongoCollection<Document> collection = database.getCollection(collectionId.name());

                    LOGGER.trace("Emitting close window for chunk = '{}'", chunkId);
                    final Document signal = new Document();
                    signal.put(DOCUMENT_ID, chunkId + "-close");
                    signal.put("type", CloseIncrementalSnapshotWindow.NAME);
                    signal.put("payload", "");
                    collection.insertOne(signal);
                });
    }
}
