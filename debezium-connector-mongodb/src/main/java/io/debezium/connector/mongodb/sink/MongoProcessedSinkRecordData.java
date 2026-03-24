/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import java.util.Optional;
import java.util.function.Supplier;

import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.WriteModel;

import io.debezium.connector.mongodb.MongoDbFieldName;
import io.debezium.connector.mongodb.sink.converters.SinkDocument;
import io.debezium.connector.mongodb.sink.converters.SinkRecordConverter;
import io.debezium.connector.mongodb.sink.eventhandler.mongodb.MongoDbEventHandler;
import io.debezium.connector.mongodb.sink.eventhandler.relational.RelationalEventHandler;
import io.debezium.data.Envelope;
import io.debezium.sink.DebeziumSinkRecord;

public class MongoProcessedSinkRecordData {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoProcessedSinkRecordData.class);
    private final MongoDbSinkConnectorConfig config;
    private final MongoNamespace namespace;
    private final DebeziumSinkRecord sinkRecord;
    private final SinkDocument sinkDocument;
    private final WriteModel<BsonDocument> writeModel;

    private Exception exception;
    private final String databaseName;

    MongoProcessedSinkRecordData(final DebeziumSinkRecord sinkRecord, final MongoDbSinkConnectorConfig sinkConfig) {
        this.sinkRecord = sinkRecord;
        this.databaseName = sinkConfig.getSinkDatabaseName();
        this.config = sinkConfig;
        this.sinkDocument = new SinkRecordConverter().convert(sinkRecord);
        this.namespace = createNamespace();
        this.writeModel = createWriteModel();
    }

    public MongoDbSinkConnectorConfig getConfig() {
        return config;
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public DebeziumSinkRecord getSinkRecord() {
        return sinkRecord;
    }

    public WriteModel<BsonDocument> getWriteModel() {
        return writeModel;
    }

    public Exception getException() {
        return exception;
    }

    public SinkDocument getSinkDocument() {
        return sinkDocument;
    }

    private MongoNamespace createNamespace() {
        return tryProcess(
                () -> Optional.of(new MongoNamespace(
                        databaseName,
                        config.getCollectionNamingStrategy().resolveCollectionName(sinkRecord, config.getCollectionNameFormat()))))
                .orElse(null);
    }

    private WriteModel<BsonDocument> createWriteModel() {
        return tryProcess(
                () -> {
                    BsonDocument valueDoc = sinkDocument.getValueDoc().orElseGet(BsonDocument::new);
                    if (isMongoDbFormat(valueDoc)) {
                        return new MongoDbEventHandler(config).handle(sinkDocument);
                    }
                    return new RelationalEventHandler(config).handle(sinkDocument);
                })
                .orElse(null);
    }

    private boolean isMongoDbFormat(BsonDocument valueDoc) {
        // Full-document update or insert: after is a JSON string (MongoDB format)
        if (valueDoc.containsKey(Envelope.FieldName.AFTER) && valueDoc.get(Envelope.FieldName.AFTER).isString()) {
            return true;
        }
        // Partial update or schemaless delete: updateDescription signals MongoDB CDC
        if (valueDoc.containsKey(MongoDbFieldName.UPDATE_DESCRIPTION)) {
            return true;
        }
        // Schema-based detection via connector namespace in schema name
        if (sinkRecord.valueSchema() != null && sinkRecord.valueSchema().name() != null
                && sinkRecord.valueSchema().name().contains("io.debezium.connector.mongodb")) {
            return true;
        }
        // Schemaless delete: key document contains only the MongoDB "id" field
        BsonDocument keyDoc = sinkDocument.getKeyDoc().orElseGet(BsonDocument::new);
        if (keyDoc.size() == 1 && keyDoc.containsKey("id")) {
            return true;
        }
        return false;
    }

    private <T> Optional<T> tryProcess(final Supplier<Optional<T>> supplier) {
        try {
            return supplier.get();
        }
        catch (Exception e) {
            exception = e;
            LOGGER.error("Unable to process record {}", sinkRecord, e);
        }
        return Optional.empty();
    }
}
