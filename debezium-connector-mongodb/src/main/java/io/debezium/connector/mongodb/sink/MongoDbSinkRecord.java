/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.mongodb.sink.converters.SinkDocument;
import io.debezium.connector.mongodb.sink.converters.SinkRecordConverter;
import io.debezium.connector.mongodb.sink.eventhandler.relational.RelationalEventHandler;

public class MongoDbSinkRecord extends KafkaDebeziumSinkRecord {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbSinkRecord.class);
    private static final ReplaceOptions REPLACE_OPTIONS = new ReplaceOptions().upsert(true);

    private final MongoDbSinkConnectorConfig config;
    private BsonDocument keyDoc;
    private BsonDocument afterDoc;
    private WriteModel<BsonDocument> writeModel;
    private Exception conversionException;

    public MongoDbSinkRecord(SinkRecord record, MongoDbSinkConnectorConfig config) {
        super(record, config.cloudEventsSchemaNamePattern());
        this.config = config;

        try {
            Struct filteredKey = getFilteredKey(config.getPrimaryKeyMode(), config.getPrimaryKeyFields(), config.fieldFilter());
            this.keyDoc = filteredKey != null ? SinkRecordConverter.convertStruct(filteredKey) : new BsonDocument();

            if (!isDelete() && !isTombstone() && !isTruncate()) {
                Struct filteredPayload = getFilteredPayload(config.fieldFilter());
                this.afterDoc = filteredPayload != null ? SinkRecordConverter.convertStruct(filteredPayload) : null;
            }

            this.writeModel = buildWriteModel();
        }
        catch (Exception e) {
            this.conversionException = e;
            LOGGER.error("Unable to process record {}", record, e);
        }
    }

    public BsonDocument getKeyDoc() {
        return keyDoc;
    }

    public BsonDocument getAfterDoc() {
        return afterDoc;
    }

    public WriteModel<BsonDocument> getWriteModel() {
        return writeModel;
    }

    public Exception getConversionException() {
        return conversionException;
    }

    public MongoDbSinkConnectorConfig getConfig() {
        return config;
    }

    private WriteModel<BsonDocument> buildWriteModel() {
        if (isTombstone() || isTruncate() || isSchemaChange()) {
            return null;
        }

        if (isDelete()) {
            if (!config.isDeleteEnabled()) {
                return null;
            }
            return config.getDeleteWriteModelStrategy().createWriteModel(new SinkDocument(keyDoc, null));
        }

        if (afterDoc == null) {
            return null;
        }

        BsonDocument filterDoc = RelationalEventHandler.generateFilterDoc(keyDoc, config.getColumnNamingStrategy());
        BsonDocument upsertDoc = RelationalEventHandler.generateUpsertOrReplaceDoc(keyDoc, afterDoc, filterDoc, config.getColumnNamingStrategy());
        return new ReplaceOneModel<>(filterDoc, upsertDoc, REPLACE_OPTIONS);
    }
}
