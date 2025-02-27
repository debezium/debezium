/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.eventhandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;

import com.mongodb.client.model.WriteModel;

import io.debezium.connector.mongodb.sink.MongoDbSinkConnectorConfig;
import io.debezium.connector.mongodb.sink.converters.SinkDocument;
import io.debezium.data.Envelope.Operation;

public abstract class EventHandler {

    private static final String OPERATION_TYPE_FIELD_PATH = "op";
    private static final String DDL_FIELD_PATH = "ddl";
    private static final EventOperation NOOP_CDC_OPERATION = (doc, columnNamingStrategy) -> null;

    private final MongoDbSinkConnectorConfig config;
    private final Map<Operation, EventOperation> operations = new HashMap<>();

    public EventHandler(final MongoDbSinkConnectorConfig config) {
        this.config = config;
    }

    public MongoDbSinkConnectorConfig getConfig() {
        return config;
    }

    public abstract Optional<WriteModel<BsonDocument>> handle(SinkDocument doc);

    protected void registerOperations(final Map<Operation, EventOperation> operations) {
        this.operations.putAll(operations);
    }

    public EventOperation getEventOperation(final BsonDocument doc) {
        try {
            if (!doc.containsKey(OPERATION_TYPE_FIELD_PATH) && doc.containsKey(DDL_FIELD_PATH)) {
                return NOOP_CDC_OPERATION;
            }

            if (!doc.containsKey(OPERATION_TYPE_FIELD_PATH)
                    || !doc.get(OPERATION_TYPE_FIELD_PATH).isString()) {
                throw new DataException("Value document is missing or CDC operation is not a string");
            }
            EventOperation op = operations.get(
                    Operation.forCode(doc.get(OPERATION_TYPE_FIELD_PATH).asString().getValue()));
            if (op == null) {
                throw new DataException(
                        "No CDC operation found in mapping for op="
                                + doc.get(OPERATION_TYPE_FIELD_PATH).asString().getValue());
            }
            return op;
        }
        catch (IllegalArgumentException exc) {
            throw new DataException("Parsing CDC operation failed", exc);
        }
    }
}
