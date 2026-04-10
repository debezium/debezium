/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.eventhandler.mongodb;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.model.WriteModel;

import io.debezium.connector.mongodb.sink.MongoDbSinkConnectorConfig;
import io.debezium.connector.mongodb.sink.converters.SinkDocument;
import io.debezium.connector.mongodb.sink.eventhandler.EventHandler;
import io.debezium.connector.mongodb.sink.eventhandler.EventOperation;
import io.debezium.data.Envelope.Operation;

/**
 * An {@link EventHandler} implementation for Debezium MongoDB source connector events.
 */
public class MongoDbEventHandler extends EventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbEventHandler.class);

    private static final Map<Operation, EventOperation> DEFAULT_OPERATIONS = new HashMap<>() {
        {
            put(Operation.CREATE, new MongoDbInsertEvent());
            put(Operation.READ, new MongoDbInsertEvent());
            put(Operation.UPDATE, new MongoDbUpdateEvent());
            put(Operation.DELETE, new MongoDbDeleteEvent());
        }
    };

    public MongoDbEventHandler(final MongoDbSinkConnectorConfig config) {
        this(config, DEFAULT_OPERATIONS);
    }

    public MongoDbEventHandler(final MongoDbSinkConnectorConfig config, final Map<Operation, EventOperation> operations) {
        super(config);
        registerOperations(operations);
    }

    @Override
    public Optional<WriteModel<BsonDocument>> handle(final SinkDocument doc) {
        BsonDocument valueDoc = doc.getValueDoc().orElseGet(BsonDocument::new);

        if (valueDoc.isEmpty()) {
            LOGGER.debug("Skipping debezium tombstone event for kafka topic compaction");
            return Optional.empty();
        }

        return Optional.ofNullable(getEventOperation(valueDoc).perform(doc, getConfig().getColumnNamingStrategy()));
    }

    /**
     * Generates a filter document for MongoDB events.
     *
     * @param keyDoc the Debezium key document
     * @return the MongoDB filter document
     */
    public static BsonDocument generateFilterDoc(final BsonDocument keyDoc) {
        if (keyDoc.containsKey("id")) {
            String idStr = keyDoc.get("id").asString().getValue();
            try {
                return BsonDocument.parse(idStr);
            }
            catch (Exception e) {
                throw new DataException("Failed to parse 'id' field from key document: " + idStr, e);
            }
        }
        throw new DataException("Key document missing required 'id' field for MongoDB event");
    }
}
