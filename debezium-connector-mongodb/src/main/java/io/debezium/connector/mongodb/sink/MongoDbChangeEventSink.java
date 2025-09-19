/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import static io.debezium.connector.mongodb.sink.MongoDbSinkConnectorTask.LOGGER;
import static io.debezium.openlineage.dataset.DatasetMetadata.TABLE_DATASET_TYPE;
import static io.debezium.openlineage.dataset.DatasetMetadata.DataStore.DATABASE;
import static io.debezium.openlineage.dataset.DatasetMetadata.DatasetKind.OUTPUT;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.kafka.common.utils.ThreadUtils;
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
import io.debezium.connector.common.DebeziumTaskState;
import io.debezium.connector.mongodb.sink.converters.SinkDocument;
import io.debezium.connector.mongodb.sink.eventhandler.relational.RelationalEventHandler;
import io.debezium.dlq.ErrorReporter;
import io.debezium.metadata.CollectionId;
import io.debezium.openlineage.ConnectorContext;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.openlineage.dataset.DatasetMetadata;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.spi.ChangeEventSink;

final class MongoDbChangeEventSink implements ChangeEventSink, AutoCloseable {

    private final MongoDbSinkConnectorConfig sinkConfig;
    private final MongoClient mongoClient;
    private final ErrorReporter errorReporter;
    private final ConnectorContext connectorContext;
    private final ExecutorService executor = Executors.newFixedThreadPool(1, ThreadUtils.createThreadFactory(this.getClass().getSimpleName() + "-%d", false));

    MongoDbChangeEventSink(
                           final MongoDbSinkConnectorConfig sinkConfig,
                           final MongoClient mongoClient,
                           final ErrorReporter errorReporter, ConnectorContext connectorContext) {
        this.sinkConfig = sinkConfig;
        this.mongoClient = mongoClient;
        this.errorReporter = errorReporter;
        this.connectorContext = connectorContext;
    }

    @SuppressWarnings("try")
    @Override
    public void close() {
        try (MongoClient autoCloseable = mongoClient) {
            // just using try-with-resources to ensure they all get closed, even in the case of
            // exceptions
        }
    }

    public Optional<CollectionId> getCollectionId(String collectionName) {
        return Optional.of(new CollectionId(collectionName));
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

        executor.submit(
                () -> DebeziumOpenLineageEmitter.emit(connectorContext, DebeziumTaskState.RUNNING,
                        List.of(extractDatasetMetadata(batch.get(0).getSinkDocument(), namespace.getFullName()))));

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

    private DatasetMetadata extractDatasetMetadata(SinkDocument sinkDocument, String collectionId) {

        BsonDocument changeEvent = RelationalEventHandler.generateUpsertOrReplaceDoc(sinkDocument.getKeyDoc().get(), sinkDocument.getValueDoc().get(),
                new BsonDocument(), sinkConfig.getColumnNamingStrategy());

        List<DatasetMetadata.FieldDefinition> fieldDefinitions = changeEvent.entrySet().stream()
                .map((entry) -> new DatasetMetadata.FieldDefinition(entry.getKey(), entry.getValue().getBsonType().toString(), ""))
                .toList();
        return new DatasetMetadata(collectionId, OUTPUT, TABLE_DATASET_TYPE, DATABASE, fieldDefinitions);
    }
}
