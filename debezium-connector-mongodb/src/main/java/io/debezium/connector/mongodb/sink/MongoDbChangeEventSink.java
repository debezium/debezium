/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import static io.debezium.connector.mongodb.sink.MongoDbSinkConnectorTask.LOGGER;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;

import io.debezium.DebeziumException;
import io.debezium.dlq.ErrorReporter;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.AbstractChangeEventSink;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.spi.ChangeEventSink;

final class MongoDbChangeEventSink extends AbstractChangeEventSink implements ChangeEventSink, AutoCloseable {

    private final MongoDbSinkConnectorConfig sinkConfig;
    private final MongoClient mongoClient;
    private final ErrorReporter errorReporter;

    MongoDbChangeEventSink(
                           final MongoDbSinkConnectorConfig sinkConfig,
                           final MongoClient mongoClient,
                           final ErrorReporter errorReporter) {
        super(sinkConfig);
        this.sinkConfig = sinkConfig;
        this.mongoClient = mongoClient;
        this.errorReporter = errorReporter;
    }

    @SuppressWarnings("try")
    @Override
    public void close() {
        try (MongoClient autoCloseable = mongoClient) {
            // just using try-with-resources to ensure they all get closed, even in the case of
            // exceptions
        }
    }

    public CollectionId getCollectionId(String collectionName) {
        return new CollectionId(collectionName);
    }

    public void execute(final Collection<SinkRecord> records) {
        try {
            trackLatestRecordTimestampOffset(records);
            if (records.isEmpty()) {
                LOGGER.debug("No sink records to process for current poll operation");
            }
            else {
                List<List<MongoProcessedSinkRecordData>> batches = MongoSinkRecordProcessor.orderedGroupByTopicAndNamespace(
                        records, sinkConfig, errorReporter);
                for (List<MongoProcessedSinkRecordData> batch : batches) {
                    bulkWriteBatch(batch);
                }
            }
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    private void trackLatestRecordTimestampOffset(final Collection<SinkRecord> records) {
        OptionalLong latestRecord = records.stream()
                .filter(v -> v.timestamp() != null)
                .mapToLong(ConnectRecord::timestamp)
                .max();
    }

    private void bulkWriteBatch(final List<MongoProcessedSinkRecordData> batch) {
        if (batch.isEmpty()) {
            return;
        }

        MongoNamespace namespace = batch.get(0).getNamespace();

        List<WriteModel<BsonDocument>> writeModels = batch.stream()
                .map(MongoProcessedSinkRecordData::getWriteModel)
                .collect(Collectors.toList());
        boolean bulkWriteOrdered = true;

        try {
            LOGGER.debug(
                    "Bulk writing {} document(s) into collection [{}] via an {} bulk write",
                    writeModels.size(),
                    namespace.getFullName(),
                    bulkWriteOrdered ? "ordered" : "unordered");
            BulkWriteResult result = mongoClient
                    .getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName(), BsonDocument.class)
                    .bulkWrite(writeModels, new BulkWriteOptions().ordered(bulkWriteOrdered));
            LOGGER.debug("Mongodb bulk write result: {}", result);
        }
        catch (RuntimeException e) {
            handleTolerableWriteException(
                    batch.stream()
                            .map(MongoProcessedSinkRecordData::getSinkRecord)
                            .collect(Collectors.toList()),
                    bulkWriteOrdered,
                    e,
                    true,
                    true);
        }
    }

    private void handleTolerableWriteException(
                                               final List<DebeziumSinkRecord> batch,
                                               final boolean ordered,
                                               final RuntimeException e,
                                               final boolean logErrors,
                                               final boolean tolerateErrors) {
        if (e instanceof MongoBulkWriteException) {
            throw new DataException(e);
        }
        else {
            if (logErrors) {
                log(batch, e);
            }
            if (tolerateErrors) {
                batch.forEach(record -> errorReporter.report(record, e));
            }
            else {
                throw new DataException(e);
            }
        }
    }

    private static void log(final Collection<DebeziumSinkRecord> records, final RuntimeException e) {
        LOGGER.error("Failed to put into the sink the following records: {}", records, e);
    }
}
