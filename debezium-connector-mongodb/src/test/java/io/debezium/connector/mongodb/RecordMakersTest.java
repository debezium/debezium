/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.DBRef;
import com.mongodb.util.JSONSerializers;

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
        source = new SourceInfo(SERVER_NAME);
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
                                       .append("h", Long.valueOf(12345678))
                                       .append("op", "i");
        RecordsForCollection records = recordMakers.forCollection(collectionId);
        records.recordEvent(event, 1002);
        assertThat(produced.size()).isEqualTo(1);
        SourceRecord record = produced.get(0);
        Struct key = (Struct) record.key();
        Struct value = (Struct) record.value();
        assertThat(key.schema()).isSameAs(record.keySchema());
        assertThat(key.get("id")).isEqualTo("{ \"$oid\" : \"" + objId + "\"}");
        assertThat(value.schema()).isSameAs(record.valueSchema());
        // assertThat(value.getString(FieldName.BEFORE)).isNull();
        assertThat(value.getString(FieldName.AFTER)).isEqualTo(obj.toJson(WRITER_SETTINGS));
        assertThat(value.getString(FieldName.OPERATION)).isEqualTo(Operation.CREATE.code());
        assertThat(value.getInt64(FieldName.TIMESTAMP)).isEqualTo(1002L);
        Struct actualSource = value.getStruct(FieldName.SOURCE);
        Struct expectedSource = source.lastOffsetStruct("rs0", collectionId);
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
                                       .append("h", Long.valueOf(12345678))
                                       .append("op", "u");
        RecordsForCollection records = recordMakers.forCollection(collectionId);
        records.recordEvent(event, 1002);
        assertThat(produced.size()).isEqualTo(1);
        SourceRecord record = produced.get(0);
        Struct key = (Struct) record.key();
        Struct value = (Struct) record.value();
        assertThat(key.schema()).isSameAs(record.keySchema());
        assertThat(key.get("id")).isEqualTo(JSONSerializers.getStrict().serialize(objId));
        assertThat(value.schema()).isSameAs(record.valueSchema());
        // assertThat(value.getString(FieldName.BEFORE)).isNull();
        assertThat(value.getString(FieldName.AFTER)).isNull();
        assertThat(value.getString("patch")).isEqualTo(obj.toJson(WRITER_SETTINGS));
        assertThat(value.getString(FieldName.OPERATION)).isEqualTo(Operation.UPDATE.code());
        assertThat(value.getInt64(FieldName.TIMESTAMP)).isEqualTo(1002L);
        Struct actualSource = value.getStruct(FieldName.SOURCE);
        Struct expectedSource = source.lastOffsetStruct("rs0", collectionId);
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
                                       .append("h", Long.valueOf(12345678))
                                       .append("op", "d");
        RecordsForCollection records = recordMakers.forCollection(collectionId);
        records.recordEvent(event, 1002);
        assertThat(produced.size()).isEqualTo(2);

        SourceRecord record = produced.get(0);
        Struct key = (Struct) record.key();
        Struct value = (Struct) record.value();
        assertThat(key.schema()).isSameAs(record.keySchema());
        assertThat(key.get("id")).isEqualTo(JSONSerializers.getStrict().serialize(objId));
        assertThat(value.schema()).isSameAs(record.valueSchema());
        assertThat(value.getString(FieldName.AFTER)).isNull();
        assertThat(value.getString("patch")).isNull();
        assertThat(value.getString(FieldName.OPERATION)).isEqualTo(Operation.DELETE.code());
        assertThat(value.getInt64(FieldName.TIMESTAMP)).isEqualTo(1002L);
        Struct actualSource = value.getStruct(FieldName.SOURCE);
        Struct expectedSource = source.lastOffsetStruct("rs0", collectionId);
        assertThat(actualSource).isEqualTo(expectedSource);

        SourceRecord tombstone = produced.get(1);
        Struct key2 = (Struct) tombstone.key();
        assertThat(key2.schema()).isSameAs(tombstone.keySchema());
        assertThat(key2.get("id")).isEqualTo(JSONSerializers.getStrict().serialize(objId));
        assertThat(tombstone.value()).isNull();
        assertThat(tombstone.valueSchema()).isNull();
    }

    @Test
    public void shouldGenerateRecordsWithCorrectlySerializedId() throws InterruptedException {
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        BsonTimestamp ts = new BsonTimestamp(1000, 1);

        // long
        Document obj = new Document().append("_id", Long.valueOf(Integer.MAX_VALUE) + 10).append("name", "Sally");

        Document event = new Document().append("o", obj)
                                       .append("ns", "dbA.c1")
                                       .append("ts", ts)
                                       .append("h", Long.valueOf(12345678))
                                       .append("op", "i");
        RecordsForCollection records = recordMakers.forCollection(collectionId);
        records.recordEvent(event, 1002);

        // String
        obj = new Document().append("_id", "123").append("name", "Sally");
        event = new Document().append("o", obj)
                .append("ns", "dbA.c1")
                .append("ts", ts)
                .append("h", Long.valueOf(12345678))
                .append("op", "i");
        records = recordMakers.forCollection(collectionId);
        records.recordEvent(event, 1003);

        // Complex key type
        obj = new Document()
                .append("_id", new Document().append("company", 32).append("dept", "home improvement"))
                .append("name", "Sally");
        event = new Document().append("o", obj)
                .append("ns", "dbA.c1")
                .append("ts", ts)
                .append("h", Long.valueOf(12345678))
                .append("op", "i");
        records = recordMakers.forCollection(collectionId);
        records.recordEvent(event, 1004);

        // date
        Calendar cal = Calendar.getInstance();
        cal.set(2017, 9, 19);

        obj = new Document()
                .append("_id", cal.getTime())
                .append("name", "Sally");
        event = new Document().append("o", obj)
                .append("ns", "dbA.c1")
                .append("ts", ts)
                .append("h", Long.valueOf(12345678))
                .append("op", "i");
        records = recordMakers.forCollection(collectionId);
        records.recordEvent(event, 1005);

        // Decimal128
        obj = new Document()
                .append("_id", new Decimal128(new BigDecimal("123.45678")))
                .append("name", "Sally");
        event = new Document().append("o", obj)
                .append("ns", "dbA.c1")
                .append("ts", ts)
                .append("h", Long.valueOf(12345678))
                .append("op", "i");
        records = recordMakers.forCollection(collectionId);
        records.recordEvent(event, 1004);

        assertThat(produced.size()).isEqualTo(5);

        SourceRecord record = produced.get(0);
        Struct key = (Struct) record.key();
        assertThat(key.get("id")).isEqualTo("2147483657");

        record = produced.get(1);
        key = (Struct) record.key();
        assertThat(key.get("id")).isEqualTo("\"123\"");

        record = produced.get(2);
        key = (Struct) record.key();
        assertThat(key.get("id")).isEqualTo("{ \"company\" : 32 , \"dept\" : \"home improvement\"}");

        record = produced.get(3);
        key = (Struct) record.key();
        // that's actually not what https://docs.mongodb.com/manual/reference/mongodb-extended-json/#date suggests;
        // seems JsonSerializers is not fully compliant with that description
        assertThat(key.get("id")).isEqualTo("{ \"$date\" : " + cal.getTime().getTime() + "}");

        record = produced.get(4);
        key = (Struct) record.key();
        assertThat(key.get("id")).isEqualTo("{ \"$numberDecimal\" : \"123.45678\"}");
    }

    @Test
    public void shouldSupportDbRef() throws InterruptedException {
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        BsonTimestamp ts = new BsonTimestamp(1000, 1);
        ObjectId objId = new ObjectId();
        Document obj = new Document().append("_id", objId)
                                     .append("name", "Sally")
                                     .append("ref", new DBRef("othercollection", 15));
        Document event = new Document().append("o", obj)
                                       .append("ns", "dbA.c1")
                                       .append("ts", ts)
                                       .append("h", Long.valueOf(12345678))
                                       .append("op", "i");
        RecordsForCollection records = recordMakers.forCollection(collectionId);
        records.recordEvent(event, 1002);
        assertThat(produced.size()).isEqualTo(1);
        SourceRecord record = produced.get(0);
        Struct key = (Struct) record.key();
        Struct value = (Struct) record.value();
        assertThat(key.schema()).isSameAs(record.keySchema());
        assertThat(key.get("id")).isEqualTo("{ \"$oid\" : \"" + objId + "\"}");
        assertThat(value.schema()).isSameAs(record.valueSchema());
        assertThat(value.getString(FieldName.AFTER)).isEqualTo("{"
                    + "\"_id\" : {\"$oid\" : \"" + objId + "\"},"
                    + "\"name\" : \"Sally\","
                    + "\"ref\" : {\"$ref\" : \"othercollection\",\"$id\" : 15}"
                + "}"
        );
        assertThat(value.getString(FieldName.OPERATION)).isEqualTo(Operation.CREATE.code());
        assertThat(value.getInt64(FieldName.TIMESTAMP)).isEqualTo(1002L);
        Struct actualSource = value.getStruct(FieldName.SOURCE);
        Struct expectedSource = source.lastOffsetStruct("rs0", collectionId);
        assertThat(actualSource).isEqualTo(expectedSource);
    }
}
