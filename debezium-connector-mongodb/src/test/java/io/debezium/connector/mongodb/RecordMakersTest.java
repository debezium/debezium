/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.datapipeline.base.mongodb.MongodbSchemaConfig;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.connector.mongodb.RecordMakers.RecordsForCollection;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.Envelope.Operation;

/**
 * @author Randall Hauch
 *
 */
public class RecordMakersTest {

    private static final String SERVER_NAME = "serverX.";
    private static final String PREFIX = SERVER_NAME + ".";
    private static final JsonWriterSettings WRITER_SETTINGS = new JsonWriterSettings(JsonMode.STRICT, "", ""); // most compact
                                                                                                               // JSON

    private SourceInfo source;
    private RecordMakers recordMakers;
    private TopicSelector topicSelector;
    private List<SourceRecord> produced;

    @Before
    public void beforeEach() {
        List<MongodbSchemaConfig> configs = new ArrayList<>();
        for (int i = 0; i != 100; ++i) {
            MongodbSchemaConfig config = new MongodbSchemaConfig();
            config.setMongodbSchemaList(new ArrayList<>());
            config.setCollectionName("c" + i);
            config.setDbName("dbA");
            configs.add(config);
        }
        source = new SourceInfo("101", SERVER_NAME, new MongoDBSchemaCache(configs), new LinkedList<>());
        topicSelector = TopicSelector.defaultSelector(PREFIX);
        produced = new ArrayList<>();
        recordMakers = new RecordMakers(source, topicSelector, produced::add);
    }

    @Test
    public void shouldAlwaysFindRecordMakerForCollection() {
        for (int i = 0; i != 100; ++i) {
            CollectionId id = new CollectionId("rs0", "dbA", "c" + i);
            RecordsForCollection records = recordMakers.forCollection(id);
            assertThat(records).isNotNull();
            assertThat(records.collectionId()).isSameAs(id);
        }
    }

    @Test
    public void shouldGenerateRecordForInsertEvent() throws InterruptedException {
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        BsonTimestamp ts = new BsonTimestamp(1000, 1);
        ObjectId objId = new ObjectId();
        Document obj = new Document().append("_id", objId).append("name", "Sally");
        Document event = new Document().append("o", obj)
                                       .append("ns", "dbA.c1")
                                       .append("ts", ts)
                                       .append("h", new Long(12345678))
                                       .append("op", "i");
        RecordsForCollection records = recordMakers.forCollection(collectionId);
        records.recordEvent(event, 1002);
        assertThat(produced.size()).isEqualTo(1);
        SourceRecord record = produced.get(0);
        Struct key = (Struct) record.key();
        Struct value = (Struct) record.value();
        assertThat(key.schema()).isSameAs(record.keySchema());
        assertThat(key.get("_id")).isEqualTo(objId.toString());
        assertThat(value.schema()).isSameAs(record.valueSchema());
        // assertThat(value.getString(FieldName.BEFORE)).isNull();
        assertThat(value.getString(FieldName.AFTER)).isEqualTo(obj.toJson(WRITER_SETTINGS));
        assertThat(value.getString(FieldName.OPERATION)).isEqualTo(Operation.CREATE.code());
        assertThat(value.getInt64(FieldName.TIMESTAMP)).isEqualTo(1002L);
        Struct actualSource = value.getStruct(FieldName.SOURCE);
        Struct expectedSource = source.lastOffsetStruct("rs0", collectionId,"", 0);
        assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    public void shouldGenerateRecordForUpdateEvent() throws InterruptedException {
        BsonTimestamp ts = new BsonTimestamp(1000, 1);
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document().append("$set", new Document("name", "Sally"));
        Document event = new Document().append("o", obj)
                                       .append("o2", objId)
                                       .append("ns", "dbA.c1")
                                       .append("ts", ts)
                                       .append("h", new Long(12345678))
                                       .append("op", "u");
        RecordsForCollection records = recordMakers.forCollection(collectionId);
        records.recordEvent(event, 1002);
        assertThat(produced.size()).isEqualTo(1);
        SourceRecord record = produced.get(0);
        Struct key = (Struct) record.key();
        Struct value = (Struct) record.value();
        assertThat(key.schema()).isSameAs(record.keySchema());
        assertThat(key.get("_id")).isEqualTo(objId.toString());
        assertThat(value.schema()).isSameAs(record.valueSchema());
        // assertThat(value.getString(FieldName.BEFORE)).isNull();
        assertThat(value.getString(FieldName.AFTER)).isNull();
        assertThat(value.getString("patch")).isEqualTo(obj.toJson(WRITER_SETTINGS));
        assertThat(value.getString(FieldName.OPERATION)).isEqualTo(Operation.UPDATE.code());
        assertThat(value.getInt64(FieldName.TIMESTAMP)).isEqualTo(1002L);
        Struct actualSource = value.getStruct(FieldName.SOURCE);
        Struct expectedSource = source.lastOffsetStruct("rs0", collectionId,"", 0);
        assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    public void shouldGenerateRecordForDeleteEvent() throws InterruptedException {
        BsonTimestamp ts = new BsonTimestamp(1000, 1);
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document("_id", objId);
        Document event = new Document().append("o", obj)
                                       .append("ns", "dbA.c1")
                                       .append("ts", ts)
                                       .append("h", new Long(12345678))
                                       .append("op", "d");
        RecordsForCollection records = recordMakers.forCollection(collectionId);
        records.recordEvent(event, 1002);
        assertThat(produced.size()).isEqualTo(2);

        SourceRecord record = produced.get(0);
        Struct key = (Struct) record.key();
        Struct value = (Struct) record.value();
        assertThat(key.schema()).isSameAs(record.keySchema());
        assertThat(key.get("_id")).isEqualTo(objId.toString());
        assertThat(value.schema()).isSameAs(record.valueSchema());
        assertThat(value.getString(FieldName.AFTER)).isNull();
        assertThat(value.getString("patch")).isNull();
        assertThat(value.getString(FieldName.OPERATION)).isEqualTo(Operation.DELETE.code());
        assertThat(value.getInt64(FieldName.TIMESTAMP)).isEqualTo(1002L);
        Struct actualSource = value.getStruct(FieldName.SOURCE);
        Struct expectedSource = source.lastOffsetStruct("rs0", collectionId,"", 0);
        assertThat(actualSource).isEqualTo(expectedSource);

        SourceRecord tombstone = produced.get(1);
        Struct key2 = (Struct) tombstone.key();
        assertThat(key2.schema()).isSameAs(tombstone.keySchema());
        assertThat(key2.get("_id")).isEqualTo(objId.toString());
        assertThat(tombstone.value()).isNull();
        assertThat(tombstone.valueSchema()).isNull();
    }

}
