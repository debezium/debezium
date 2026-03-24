/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.eventhandler.mongodb;

import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

import io.debezium.connector.mongodb.MongoDbFieldName;
import io.debezium.connector.mongodb.sink.converters.SinkDocument;
import io.debezium.connector.mongodb.sink.eventhandler.EventOperation;
import io.debezium.data.Envelope;
import io.debezium.sink.naming.ColumnNamingStrategy;

/**
 * An {@link EventOperation} implementation for Debezium MongoDB source connector update events.
 */
public class MongoDbUpdateEvent implements EventOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbUpdateEvent.class);

    @Override
    public WriteModel<BsonDocument> perform(SinkDocument doc, ColumnNamingStrategy columnNamingStrategy) {
        BsonDocument valueDoc = doc.getValueDoc()
                .orElseThrow(() -> new DataException("Value document is missing for update operation"));

        BsonDocument keyDoc = doc.getKeyDoc()
                .orElseThrow(() -> new DataException("Key document is missing for update operation"));

        BsonDocument filterDoc = MongoDbEventHandler.generateFilterDoc(keyDoc);

        if (valueDoc.containsKey(Envelope.FieldName.AFTER) && valueDoc.get(Envelope.FieldName.AFTER).isString()) {
            String afterStr = valueDoc.get(Envelope.FieldName.AFTER).asString().getValue();
            BsonDocument afterDoc;
            try {
                afterDoc = BsonDocument.parse(afterStr);
            }
            catch (RuntimeException e) {
                throw new DataException("Failed to parse '" + Envelope.FieldName.AFTER + "' field as JSON for update operation", e);
            }
            return new ReplaceOneModel<>(filterDoc, afterDoc, new ReplaceOptions().upsert(true));
        }

        if (valueDoc.containsKey(MongoDbFieldName.UPDATE_DESCRIPTION)) {
            BsonValue updateDescriptionValue = valueDoc.get(MongoDbFieldName.UPDATE_DESCRIPTION);
            if (!updateDescriptionValue.isDocument()) {
                throw new DataException("Update description field must be a document");
            }

            BsonDocument updateDescription = updateDescriptionValue.asDocument();
            BsonDocument updateClause = new BsonDocument();

            if (updateDescription.containsKey(MongoDbFieldName.UPDATED_FIELDS)) {
                BsonValue updatedFieldsValue = updateDescription.get(MongoDbFieldName.UPDATED_FIELDS);
                if (updatedFieldsValue.isString()) {
                    updateClause.append("$set", BsonDocument.parse(updatedFieldsValue.asString().getValue()));
                }
            }

            if (updateDescription.containsKey(MongoDbFieldName.REMOVED_FIELDS)) {
                BsonValue removedFieldsValue = updateDescription.get(MongoDbFieldName.REMOVED_FIELDS);
                if (removedFieldsValue.isArray()) {
                    BsonArray removedFieldsArray = removedFieldsValue.asArray();
                    BsonDocument unsetClause = new BsonDocument();
                    for (BsonValue removedField : removedFieldsArray) {
                        if (removedField.isString()) {
                            unsetClause.append(removedField.asString().getValue(), new BsonString(""));
                        }
                    }
                    if (!unsetClause.isEmpty()) {
                        updateClause.append("$unset", unsetClause);
                    }
                }
            }

            if (!updateClause.isEmpty()) {
                return new UpdateOneModel<>(filterDoc, updateClause, new UpdateOptions().upsert(true));
            }

            // updateDescription present but only contained unsupported fields (e.g. truncatedArrays).
            // Return null so MongoProcessedSinkRecordData treats this as a no-op (Optional.empty()),
            // avoiding an unnecessary empty write to MongoDB.
            LOGGER.debug("Skipping update event with no actionable fields (e.g. truncatedArrays-only); treating as no-op");
            return null;
        }

        throw new DataException("Value document missing required update information (after or updateDescription)");
    }
}
