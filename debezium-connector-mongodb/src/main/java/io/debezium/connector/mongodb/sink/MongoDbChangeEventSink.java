/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import static io.debezium.openlineage.dataset.DatasetMetadata.TABLE_DATASET_TYPE;
import static io.debezium.openlineage.dataset.DatasetMetadata.DataStore.DATABASE;
import static io.debezium.openlineage.dataset.DatasetMetadata.DatasetKind.OUTPUT;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;

import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketException;
import com.mongodb.client.model.WriteModel;

import io.debezium.connector.common.DebeziumTaskState;
import io.debezium.connector.mongodb.connection.client.MongoDbClient;
import io.debezium.connector.mongodb.sink.eventhandler.relational.RelationalEventHandler;
import io.debezium.dlq.ErrorReporter;
import io.debezium.metadata.CollectionId;
import io.debezium.openlineage.ConnectorContext;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.openlineage.dataset.DatasetMetadata;
import io.debezium.sink.AbstractChangeEventSink;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.batch.Batch;
import io.debezium.sink.spi.ChangeEventSink;

final class MongoDbChangeEventSink extends AbstractChangeEventSink implements ChangeEventSink, AutoCloseable {

    private final MongoDbSinkConnectorConfig sinkConfig;
    private final MongoDbClient mongoClient;
    private final ErrorReporter errorReporter;
    private final ConnectorContext connectorContext;

    private final MongoDbWriter mongoDbWriter;

    MongoDbChangeEventSink(
                           final MongoDbSinkConnectorConfig sinkConfig,
                           final MongoDbClient mongoClient,
                           final ErrorReporter errorReporter, ConnectorContext connectorContext) {
        super(sinkConfig, errorReporter);
        this.sinkConfig = sinkConfig;
        this.mongoClient = mongoClient;
        this.errorReporter = errorReporter;
        this.connectorContext = connectorContext;
        this.mongoDbWriter = new MongoDbWriter(mongoClient, errorReporter);
    }

    @Override
    protected DebeziumSinkRecord createSinkRecord(SinkRecord kafkaSinkRecord) {
        return new MongoDbSinkRecord(kafkaSinkRecord, sinkConfig);
    }

    @Override
    protected void doWriteBatch(Batch batch) {
        if (batch.isEmpty()) {
            return;
        }

        Map<String, List<WriteModel<BsonDocument>>> groupedByCollection = new LinkedHashMap<>();
        Map<String, MongoDbSinkRecord> firstRecordByCollection = new LinkedHashMap<>();
        Set<String> truncatedCollections = new LinkedHashSet<>();

        for (var batchRecord : batch) {
            MongoDbSinkRecord record = (MongoDbSinkRecord) batchRecord.record();

            if (record.getConversionException() != null) {
                errorReporter.report(record, record.getConversionException());
                continue;
            }

            if (record.isTruncate()) {
                truncatedCollections.add(batchRecord.collectionId().identifier());
                continue;
            }

            WriteModel<BsonDocument> model = record.getWriteModel();
            if (model != null) {
                String collectionName = batchRecord.collectionId().identifier();
                groupedByCollection.computeIfAbsent(collectionName, k -> new ArrayList<>()).add(model);
                firstRecordByCollection.putIfAbsent(collectionName, record);
            }
        }

        for (String collectionName : truncatedCollections) {
            MongoNamespace namespace = new MongoNamespace(sinkConfig.getSinkDatabaseName(), collectionName);
            mongoDbWriter.deleteAll(namespace);
        }

        for (var entry : groupedByCollection.entrySet()) {
            MongoNamespace namespace = new MongoNamespace(sinkConfig.getSinkDatabaseName(), entry.getKey());
            emitDatasetMetadata(firstRecordByCollection.get(entry.getKey()), namespace);
            mongoDbWriter.bulkWriteBatch(namespace, entry.getValue());
        }
    }

    public CollectionId getCollectionId(String collectionName) {
        return new CollectionId(collectionName);
    }

    @Override
    protected boolean isRetriableWriteException(RuntimeException exception) {
        Throwable cause = exception;
        while (cause != null) {
            if (cause instanceof MongoException mongoEx) {
                if (mongoEx.hasErrorLabel(MongoException.RETRYABLE_ERROR_LABEL)
                        || mongoEx.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                    return true;
                }
            }
            if (cause instanceof MongoSocketException
                    || cause instanceof MongoNotPrimaryException
                    || cause instanceof MongoNodeIsRecoveringException
                    || cause instanceof MongoConnectionPoolClearedException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    @SuppressWarnings("try")
    @Override
    public void close() {
        try (MongoDbClient autoCloseable = mongoClient) {
        }
    }

    private void emitDatasetMetadata(MongoDbSinkRecord record, MongoNamespace namespace) {
        BsonDocument keyDoc = record.getKeyDoc() != null ? record.getKeyDoc() : new BsonDocument();
        BsonDocument afterDoc = record.getAfterDoc() != null ? record.getAfterDoc() : new BsonDocument();

        BsonDocument changeEvent = RelationalEventHandler.generateUpsertOrReplaceDoc(
                keyDoc, afterDoc, new BsonDocument(), sinkConfig.getColumnNamingStrategy());

        List<DatasetMetadata.FieldDefinition> fieldDefinitions = changeEvent.entrySet().stream()
                .map((entry) -> new DatasetMetadata.FieldDefinition(entry.getKey(), entry.getValue().getBsonType().toString(), ""))
                .toList();

        DebeziumOpenLineageEmitter.emit(connectorContext,
                DebeziumTaskState.RUNNING,
                List.of(new DatasetMetadata(namespace.getFullName(), OUTPUT, TABLE_DATASET_TYPE, DATABASE, fieldDefinitions)));
    }
}
