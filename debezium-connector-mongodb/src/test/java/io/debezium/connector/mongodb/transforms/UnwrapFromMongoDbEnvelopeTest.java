/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.mongodb.CollectionId;
import io.debezium.connector.mongodb.RecordMakers;
import io.debezium.connector.mongodb.RecordMakers.RecordsForCollection;
import io.debezium.connector.mongodb.SourceInfo;
import io.debezium.connector.mongodb.TopicSelector;
import io.debezium.doc.FixFor;

/**
 * Unit test for {@link UnwrapFromMongoDbEnvelope}. It uses {@link RecordMakers}
 * to assemble source records as the connector would emit them and feeds them to
 * the SMT.
 *
 * @author Gunnar Morling
 */
public class UnwrapFromMongoDbEnvelopeTest {

    private static final String SERVER_NAME = "serverX.";
    private static final String PREFIX = SERVER_NAME + ".";

    private SourceInfo source;
    private RecordMakers recordMakers;
    private TopicSelector topicSelector;
    private List<SourceRecord> produced;

    private UnwrapFromMongoDbEnvelope<SourceRecord> transformation;

    @Before
    public void setup() {
        source = new SourceInfo(SERVER_NAME);
        topicSelector = TopicSelector.defaultSelector(PREFIX);
        produced = new ArrayList<>();
        recordMakers = new RecordMakers(source, topicSelector, produced::add, true);

        transformation = new UnwrapFromMongoDbEnvelope<SourceRecord>();
        transformation.configure(Collections.emptyMap());
    }

    @After
    public void closeSmt() {
        transformation.close();
    }

    @Test
    public void shouldTransformRecordForInsertEvent() throws InterruptedException {
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        BsonTimestamp ts = new BsonTimestamp(1000, 1);
        ObjectId objId = new ObjectId();
        Document obj = new Document().append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        // given
        Document event = new Document().append("o", obj)
                                       .append("ns", "dbA.c1")
                                       .append("ts", ts)
                                       .append("h", Long.valueOf(12345678))
                                       .append("op", "i");
        RecordsForCollection records = recordMakers.forCollection(collectionId);
        records.recordEvent(event, 1002);
        assertThat(produced.size()).isEqualTo(1);
        SourceRecord record = produced.get(0);

        // when
        SourceRecord transformed = transformation.apply(record);

        Struct key = (Struct) transformed.key();
        Struct value = (Struct) transformed.value();

        // then assert key and its schema
        assertThat(key.schema()).isSameAs(transformed.keySchema());
        assertThat(key.schema().field("id").schema()).isEqualTo(SchemaBuilder.OPTIONAL_STRING_SCHEMA);
        assertThat(key.get("id")).isEqualTo(objId.toString());

        // and then assert value and its schema
        assertThat(value.schema()).isSameAs(transformed.valueSchema());
        assertThat(value.get("name")).isEqualTo("Sally");
        assertThat(value.get("id")).isEqualTo(objId.toString());
        assertThat(value.get("phone")).isEqualTo(123L);
        assertThat(value.get("active")).isEqualTo(true);
        assertThat(value.get("scores")).isEqualTo(Arrays.asList(1.2, 3.4, 5.6));

        assertThat(value.schema().field("id").schema()).isEqualTo(SchemaBuilder.OPTIONAL_STRING_SCHEMA);
        assertThat(value.schema().field("name").schema()).isEqualTo(SchemaBuilder.OPTIONAL_STRING_SCHEMA);
        assertThat(value.schema().field("phone").schema()).isEqualTo(SchemaBuilder.OPTIONAL_INT64_SCHEMA);
        assertThat(value.schema().field("active").schema()).isEqualTo(SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA);
        assertThat(value.schema().field("scores").schema()).isEqualTo(SchemaBuilder.array(SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA).build());
        assertThat(value.schema().fields()).hasSize(5);

        transformation.close();
    }

    @Test
    public void shouldTransformRecordForInsertEventWithComplexIdType() throws InterruptedException {
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        BsonTimestamp ts = new BsonTimestamp(1000, 1);
        Document obj = new Document().append("_id", new Document().append("company", 32).append("dept", "home improvement"))
                .append("name", "Sally");

        // given
        Document event = new Document().append("o", obj)
                                       .append("ns", "dbA.c1")
                                       .append("ts", ts)
                                       .append("h", Long.valueOf(12345678))
                                       .append("op", "i");
        RecordsForCollection records = recordMakers.forCollection(collectionId);
        records.recordEvent(event, 1002);
        assertThat(produced.size()).isEqualTo(1);
        SourceRecord record = produced.get(0);

        // when
        SourceRecord transformed = transformation.apply(record);

        Struct key = (Struct) transformed.key();
        Struct value = (Struct) transformed.value();

        // then assert key and its schema
        assertThat(key.schema()).isSameAs(transformed.keySchema());
        assertThat(key.schema().field("id").schema().field("company").schema()).isEqualTo(SchemaBuilder.OPTIONAL_INT32_SCHEMA);
        assertThat(key.schema().field("id").schema().field("dept").schema()).isEqualTo(SchemaBuilder.OPTIONAL_STRING_SCHEMA);
        assertThat(((Struct)key.get("id")).get("company")).isEqualTo(32);
        assertThat(((Struct)key.get("id")).get("dept")).isEqualTo("home improvement");

        // and then assert value and its schema
        assertThat(value.schema()).isSameAs(transformed.valueSchema());
        assertThat(((Struct)value.get("id")).get("company")).isEqualTo(32);
        assertThat(((Struct)value.get("id")).get("dept")).isEqualTo("home improvement");
        assertThat(value.get("name")).isEqualTo("Sally");

        assertThat(value.schema().field("id").schema().field("company").schema()).isEqualTo(SchemaBuilder.OPTIONAL_INT32_SCHEMA);
        assertThat(value.schema().field("id").schema().field("dept").schema()).isEqualTo(SchemaBuilder.OPTIONAL_STRING_SCHEMA);
        assertThat(value.schema().field("name").schema()).isEqualTo(SchemaBuilder.OPTIONAL_STRING_SCHEMA);
        assertThat(value.schema().fields()).hasSize(2);

        transformation.close();
    }
    @Test
    public void shouldGenerateRecordForUpdateEvent() throws InterruptedException {
        BsonTimestamp ts = new BsonTimestamp(1000, 1);
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document().append("$set", new Document("name", "Sally"));

        // given
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

        // when
        SourceRecord transformed = transformation.apply(record);

        Struct key = (Struct) transformed.key();
        Struct value = (Struct) transformed.value();

        // then assert key and its schema
        assertThat(key.schema()).isSameAs(transformed.keySchema());
        assertThat(key.schema().field("id").schema()).isEqualTo(SchemaBuilder.OPTIONAL_STRING_SCHEMA);
        assertThat(key.get("id")).isEqualTo(objId.toString());

        // and then assert value and its schema
        assertThat(value.schema()).isSameAs(transformed.valueSchema());
        assertThat(value.get("name")).isEqualTo("Sally");
        assertThat(value.get("id")).isEqualTo(objId.toString());

        assertThat(value.schema().field("id").schema()).isEqualTo(SchemaBuilder.OPTIONAL_STRING_SCHEMA);
        assertThat(value.schema().field("name").schema()).isEqualTo(SchemaBuilder.OPTIONAL_STRING_SCHEMA);
        assertThat(value.schema().fields()).hasSize(2);
    }


    @Test
    @FixFor("DBZ-582")
    public void shouldGenerateRecordForDeleteEventWithoutTombstone() throws InterruptedException {
        RecordMakers recordMakers = new RecordMakers(source, topicSelector, produced::add, false);

        BsonTimestamp ts = new BsonTimestamp(1000, 1);
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document("_id", objId);

        // given
        Document event = new Document().append("o", obj)
                .append("ns", "dbA.c1")
                .append("ts", ts)
                .append("h", Long.valueOf(12345678))
                .append("op", "d");
        RecordsForCollection records = recordMakers.forCollection(collectionId);
        records.recordEvent(event, 1002);
        assertThat(produced.size()).isEqualTo(1);

        SourceRecord record = produced.get(0);

        // when
        SourceRecord transformed = transformation.apply(record);

        Struct key = (Struct) transformed.key();
        Struct value = (Struct) transformed.value();

        // then assert key and its schema
        assertThat(key.schema()).isSameAs(transformed.keySchema());
        assertThat(key.schema().field("id").schema()).isEqualTo(SchemaBuilder.OPTIONAL_STRING_SCHEMA);
        assertThat(key.get("id")).isEqualTo(objId.toString());

        assertThat(value).isNull();
    }

    @Test
    public void shouldGenerateRecordForDeleteEvent() throws InterruptedException {
        BsonTimestamp ts = new BsonTimestamp(1000, 1);
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document("_id", objId);

        // given
        Document event = new Document().append("o", obj)
                                       .append("ns", "dbA.c1")
                                       .append("ts", ts)
                                       .append("h", Long.valueOf(12345678))
                                       .append("op", "d");
        RecordsForCollection records = recordMakers.forCollection(collectionId);
        records.recordEvent(event, 1002);
        assertThat(produced.size()).isEqualTo(2);

        SourceRecord record = produced.get(0);

        // when
        SourceRecord transformed = transformation.apply(record);

        Struct key = (Struct) transformed.key();
        Struct value = (Struct) transformed.value();

        // then assert key and its schema
        assertThat(key.schema()).isSameAs(transformed.keySchema());
        assertThat(key.schema().field("id").schema()).isEqualTo(SchemaBuilder.OPTIONAL_STRING_SCHEMA);
        assertThat(key.get("id")).isEqualTo(objId.toString());

        assertThat(value).isNull();
    }
}
