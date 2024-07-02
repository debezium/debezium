/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.eventhandler.relational;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.model.WriteModel;

import io.debezium.connector.mongodb.sink.MongoDbSinkConnectorConfig;
import io.debezium.connector.mongodb.sink.converters.SinkDocument;
import io.debezium.connector.mongodb.sink.eventhandler.EventHandler;
import io.debezium.connector.mongodb.sink.eventhandler.EventOperation;
import io.debezium.data.Envelope.Operation;
import io.debezium.table.ColumnNamingStrategy;

public class RelationalEventHandler extends EventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelationalEventHandler.class);

    private static final String ID_FIELD = "_id";
    private static final String JSON_DOC_BEFORE_FIELD = "before";
    private static final String JSON_DOC_AFTER_FIELD = "after";

    private static final Map<Operation, EventOperation> DEFAULT_OPERATIONS = new HashMap<>() {
        {
            put(Operation.CREATE, new RelationalInsertEvent());
            put(Operation.READ, new RelationalInsertEvent());
            put(Operation.UPDATE, new RelationalUpdateEvent());
            put(Operation.DELETE, new RelationalDeleteEvent());
        }
    };

    public RelationalEventHandler(final MongoDbSinkConnectorConfig config) {
        this(config, DEFAULT_OPERATIONS);
    }

    public RelationalEventHandler(final MongoDbSinkConnectorConfig config, final Map<Operation, EventOperation> operations) {
        super(config);
        registerOperations(operations);
    }

    @Override
    public Optional<WriteModel<BsonDocument>> handle(final SinkDocument doc) {
        BsonDocument keyDoc = doc.getKeyDoc().orElseGet(BsonDocument::new);
        BsonDocument valueDoc = doc.getValueDoc().orElseGet(BsonDocument::new);

        if (valueDoc.isEmpty()) {
            LOGGER.debug("Skipping debezium tombstone event for kafka topic compaction");
            return Optional.empty();
        }

        return Optional.ofNullable(getEventOperation(valueDoc).perform(new SinkDocument(keyDoc, valueDoc), getConfig().getColumnNamingStrategy()));
    }

    static BsonDocument generateFilterDoc(final BsonDocument keyDoc, final BsonDocument valueDoc, final Operation opType, ColumnNamingStrategy columnNamingStrategy) {
        if (keyDoc.keySet().isEmpty()) {
            if (opType.equals(Operation.CREATE) || opType.equals(Operation.READ)) {
                // create: no PK info in keyDoc -> generate ObjectId
                return new BsonDocument(ID_FIELD, new BsonObjectId());
            }
            // update or delete: no PK info in keyDoc -> take everything in 'before' field
            try {
                BsonDocument filter = valueDoc.getDocument(JSON_DOC_BEFORE_FIELD);
                if (filter.isEmpty()) {
                    throw new BsonInvalidOperationException("value doc before field is empty");
                }
                return filter;
            }
            catch (BsonInvalidOperationException exc) {
                throw new DataException(
                        "Value doc 'before' field is empty or has invalid type"
                                + " for update/delete operation.  -> defensive actions taken!",
                        exc);
            }
        }
        // build filter document composed of all PK columns
        BsonDocument pk = new BsonDocument();
        for (String f : keyDoc.keySet()) {
            pk.put(columnNamingStrategy.resolveColumnName(f), keyDoc.get(f));
        }
        return new BsonDocument(ID_FIELD, pk);
    }

    static BsonDocument generateUpsertOrReplaceDoc(final BsonDocument keyDoc, final BsonDocument valueDoc, final BsonDocument filterDoc,
                                                   ColumnNamingStrategy columnNamingStrategy) {

        if (!valueDoc.containsKey(JSON_DOC_AFTER_FIELD)
                || valueDoc.get(JSON_DOC_AFTER_FIELD).isNull()
                || !valueDoc.get(JSON_DOC_AFTER_FIELD).isDocument()
                || valueDoc.getDocument(JSON_DOC_AFTER_FIELD).isEmpty()) {
            throw new DataException(
                    "Value document must contain non-empty 'after' field"
                            + " of type document for insert/update operation");
        }

        BsonDocument upsertDoc = new BsonDocument();
        if (filterDoc.containsKey(ID_FIELD)) {
            upsertDoc.put(ID_FIELD, filterDoc.get(ID_FIELD));
        }

        BsonDocument afterDoc = valueDoc.getDocument(JSON_DOC_AFTER_FIELD);
        for (String f : afterDoc.keySet()) {
            if (!keyDoc.containsKey(f)) {
                upsertDoc.put(columnNamingStrategy.resolveColumnName(f), afterDoc.get(f));
            }
        }
        return upsertDoc;
    }
}
