/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.WriteModel;

import io.debezium.connector.mongodb.sink.converters.SinkDocument;
import io.debezium.connector.mongodb.sink.converters.SinkRecordConverter;
import io.debezium.connector.mongodb.sink.eventhandler.relational.RelationalEventHandler;

public class MongoProcessedSinkRecordData {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoProcessedSinkRecordData.class);
    private final MongoDbSinkConnectorConfig config;
    private final MongoNamespace namespace;
    private final SinkRecord sinkRecord;
    private final SinkDocument sinkDocument;
    private final WriteModel<BsonDocument> writeModel;

    private Exception exception;
    private final String databaseName;

    MongoProcessedSinkRecordData(final SinkRecord sinkRecord, final MongoDbSinkConnectorConfig sinkConfig) {
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

    public SinkRecord getSinkRecord() {
        return sinkRecord;
    }

    public WriteModel<BsonDocument> getWriteModel() {
        return writeModel;
    }

    public Exception getException() {
        return exception;
    }

    private MongoNamespace createNamespace() {

        return tryProcess(
                () -> Optional.of(new MongoNamespace(databaseName, config.getCollectionNamingStrategy().resolveCollectionName(config, sinkRecord))))
                .orElse(null);
    }

    private WriteModel<BsonDocument> createWriteModel() {
        return tryProcess(
                () -> new RelationalEventHandler(config).handle(sinkDocument))
                .orElse(null);
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
