/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.eventhandler.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;

import io.debezium.connector.mongodb.sink.MongoDbSinkConnectorConfig;
import io.debezium.connector.mongodb.sink.converters.SinkDocument;
import io.debezium.sink.naming.DefaultColumnNamingStrategy;

/**
 * Unit tests for {@link MongoDbEventHandler}.
 */
public class MongoDbEventHandlerTest {

    private MongoDbEventHandler eventHandler;
    private MongoDbSinkConnectorConfig config;

    @BeforeEach
    public void setUp() {
        config = mock(MongoDbSinkConnectorConfig.class);
        when(config.getColumnNamingStrategy()).thenReturn(new DefaultColumnNamingStrategy());
        eventHandler = new MongoDbEventHandler(config);
    }

    @Test
    public void testInsertEvent() {
        BsonDocument keyDoc = new BsonDocument("id", new BsonString("{\"_id\": 123}"));
        BsonDocument valueDoc = new BsonDocument()
                .append("op", new BsonString("c"))
                .append("after", new BsonString("{\"_id\": 123, \"name\": \"test\"}"));

        SinkDocument sinkDoc = new SinkDocument(keyDoc, valueDoc);
        Optional<WriteModel<BsonDocument>> result = eventHandler.handle(sinkDoc);

        assertTrue(result.isPresent());
        assertTrue(result.get() instanceof ReplaceOneModel);
        ReplaceOneModel<BsonDocument> model = (ReplaceOneModel<BsonDocument>) result.get();
        assertEquals(new BsonDocument("_id", new BsonInt32(123)), model.getFilter());
        assertEquals(new BsonDocument("_id", new BsonInt32(123)).append("name", new BsonString("test")), model.getReplacement());
        assertTrue(model.getReplaceOptions().isUpsert());
    }

    @Test
    public void testUpdateEventFullDoc() {
        BsonDocument keyDoc = new BsonDocument("id", new BsonString("{\"_id\": 123}"));
        BsonDocument valueDoc = new BsonDocument()
                .append("op", new BsonString("u"))
                .append("after", new BsonString("{\"_id\": 123, \"name\": \"updated\"}"));

        SinkDocument sinkDoc = new SinkDocument(keyDoc, valueDoc);
        Optional<WriteModel<BsonDocument>> result = eventHandler.handle(sinkDoc);

        assertTrue(result.isPresent());
        assertTrue(result.get() instanceof ReplaceOneModel);
        ReplaceOneModel<BsonDocument> model = (ReplaceOneModel<BsonDocument>) result.get();
        assertEquals(new BsonDocument("_id", new BsonInt32(123)), model.getFilter());
        assertEquals(new BsonDocument("_id", new BsonInt32(123)).append("name", new BsonString("updated")), model.getReplacement());
        assertTrue(model.getReplaceOptions().isUpsert());
    }

    @Test
    public void testUpdateEventPatch() {
        BsonDocument keyDoc = new BsonDocument("id", new BsonString("{\"_id\": 123}"));
        BsonDocument updateDescription = new BsonDocument()
                .append("updatedFields", new BsonString("{\"name\": \"patched\"}"))
                .append("removedFields", new BsonArray(Collections.singletonList(new BsonString("oldField"))));
        BsonDocument valueDoc = new BsonDocument()
                .append("op", new BsonString("u"))
                .append("updateDescription", updateDescription);

        SinkDocument sinkDoc = new SinkDocument(keyDoc, valueDoc);
        Optional<WriteModel<BsonDocument>> result = eventHandler.handle(sinkDoc);

        assertTrue(result.isPresent());
        assertTrue(result.get() instanceof UpdateOneModel);
        UpdateOneModel<BsonDocument> model = (UpdateOneModel<BsonDocument>) result.get();
        assertEquals(new BsonDocument("_id", new BsonInt32(123)), model.getFilter());

        BsonDocument update = (BsonDocument) model.getUpdate();
        assertEquals(new BsonDocument("name", new BsonString("patched")), update.getDocument("$set"));
        assertEquals(new BsonDocument("oldField", new BsonString("")), update.getDocument("$unset"));
        assertTrue(model.getOptions().isUpsert());
    }

    @Test
    public void testDeleteEvent() {
        BsonDocument keyDoc = new BsonDocument("id", new BsonString("{\"_id\": 123}"));
        BsonDocument valueDoc = new BsonDocument()
                .append("op", new BsonString("d"));

        SinkDocument sinkDoc = new SinkDocument(keyDoc, valueDoc);
        Optional<WriteModel<BsonDocument>> result = eventHandler.handle(sinkDoc);

        assertTrue(result.isPresent());
        assertTrue(result.get() instanceof DeleteOneModel);
        DeleteOneModel<BsonDocument> model = (DeleteOneModel<BsonDocument>) result.get();
        assertEquals(new BsonDocument("_id", new BsonInt32(123)), model.getFilter());
    }

    @Test
    public void testTombstoneEvent() {
        BsonDocument keyDoc = new BsonDocument("id", new BsonString("{\"_id\": 123}"));
        BsonDocument valueDoc = new BsonDocument();

        SinkDocument sinkDoc = new SinkDocument(keyDoc, valueDoc);
        Optional<WriteModel<BsonDocument>> result = eventHandler.handle(sinkDoc);

        assertFalse(result.isPresent());
    }
}
