/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import static io.debezium.connector.mongodb.sink.MongoDbSinkConnectorTask.LOGGER;

import java.util.List;

import org.bson.BsonDocument;

import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;

import io.debezium.connector.mongodb.connection.client.MongoDbClient;
import io.debezium.dlq.ErrorReporter;

public class MongoDbWriter {

    private final MongoDbClient mongoClient;
    private final ErrorReporter errorReporter;

    public MongoDbWriter(MongoDbClient mongoClient, ErrorReporter errorReporter) {
        this.mongoClient = mongoClient;
        this.errorReporter = errorReporter;
    }

    public void deleteAll(MongoNamespace namespace) {
        LOGGER.warn("Truncating collection [{}] by deleting all documents. This may generate significant oplog traffic on large collections.",
                namespace.getFullName());
        mongoClient.getClient()
                .getDatabase(namespace.getDatabaseName())
                .getCollection(namespace.getCollectionName(), BsonDocument.class)
                .deleteMany(new BsonDocument());
    }

    public void bulkWriteBatch(MongoNamespace namespace, List<WriteModel<BsonDocument>> writeModels) {
        boolean bulkWriteOrdered = true;

        LOGGER.debug(
                "Bulk writing {} document(s) into collection [{}] via an {} bulk write",
                writeModels.size(),
                namespace.getFullName(),
                bulkWriteOrdered ? "ordered" : "unordered");

        BulkWriteResult result = mongoClient.getClient()
                .getDatabase(namespace.getDatabaseName())
                .getCollection(namespace.getCollectionName(), BsonDocument.class)
                .bulkWrite(writeModels, new BulkWriteOptions().ordered(bulkWriteOrdered));

        LOGGER.debug("Mongodb bulk write result: {}", result);
    }
}
