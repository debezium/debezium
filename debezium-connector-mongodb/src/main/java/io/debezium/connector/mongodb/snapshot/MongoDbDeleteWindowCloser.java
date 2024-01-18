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
import io.debezium.connector.mongodb.MongoDbPartition;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.pipeline.source.snapshot.incremental.WatermarkWindowCloser;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;

public class MongoDbDeleteWindowCloser implements WatermarkWindowCloser {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbDeleteWindowCloser.class);
    private static final String DOCUMENT_ID = "_id";

    private final MongoDbConnection mongoDbConnection;
    private final CollectionId collectionId;
    private final MongoDbIncrementalSnapshotChangeEventSource incrementalSnapshotChangeEventSource;

    public MongoDbDeleteWindowCloser(MongoDbConnection mongoDbConnection, CollectionId collectionId,
                                     MongoDbIncrementalSnapshotChangeEventSource incrementalSnapshotChangeEventSource) {
        this.mongoDbConnection = mongoDbConnection;
        this.collectionId = collectionId;
        this.incrementalSnapshotChangeEventSource = incrementalSnapshotChangeEventSource;
    }

    @Override
    public void closeWindow(Partition partition, OffsetContext offsetContext, String chunkId) throws InterruptedException {

        mongoDbConnection.execute(
                "Deleting open window for chunk '" + chunkId + "'",
                client -> {
                    final MongoDatabase database = client.getDatabase(collectionId.dbName());
                    final MongoCollection<Document> collection = database.getCollection(collectionId.name());

                    LOGGER.trace("Deleting open window for chunk = '{}'", chunkId);
                    final Document signal = new Document();
                    signal.put(DOCUMENT_ID, chunkId + "-open");
                    collection.deleteOne(signal);
                });

        // Since the close event is not written into signal data collection we need to explicit close the window.
        try {
            incrementalSnapshotChangeEventSource.closeWindow((MongoDbPartition) partition, chunkId + "-close", offsetContext);
        }
        catch (InterruptedException e) {
            LOGGER.warn("Failed to close window {} successful.", chunkId);
            Thread.currentThread().interrupt();
        }
    }
}
