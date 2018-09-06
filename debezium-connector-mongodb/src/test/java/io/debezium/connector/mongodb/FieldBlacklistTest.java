/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import io.debezium.schema.TopicSelector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.debezium.data.Envelope.FieldName.AFTER;
import static org.fest.assertions.Assertions.assertThat;

public class FieldBlacklistTest {

    private static final String SERVER_NAME = "serverX";
    private static final String PATCH = "patch";
    private static final JsonWriterSettings WRITER_SETTINGS =
            new JsonWriterSettings(JsonMode.STRICT, "", ""); // most compact JSON

    private Configurator build;
    private SourceInfo source;
    private TopicSelector<CollectionId> topicSelector;

    @Before
    public void setup() {
        build = new Configurator();
        source = new SourceInfo(SERVER_NAME);
        topicSelector = MongoDbTopicSelector.defaultSelector(SERVER_NAME, "__debezium-heartbeat");
    }

    @Test
    public void shouldNotExcludeFieldsForEventOfOtherCollection() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));
        Filters filters = build.excludeFields("*.c2.name,*.c2.active").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordObject(collectionId, obj, 1002);

        // then
        Struct value = getValue(produced);
        assertThat(value.get(AFTER)).isEqualTo(obj.toJson(WRITER_SETTINGS));
    }

    @Test
    public void shouldExcludeFieldsForReadEvent() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));
        Filters filters = build.excludeFields("*.c1.name,*.c1.active").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordObject(collectionId, obj, 1002);

        // then
        String expected = "{"
                +     "\"_id\" : {\"$oid\" : \"" + objId + "\"},"
                +     "\"phone\" : {\"$numberLong\" : \"123\"},"
                +     "\"scores\" : [1.2, 3.4, 5.6]"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(AFTER)).isEqualTo(expected);
    }

    @Test
    public void shouldNotExcludeMissingFieldsForReadEvent() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));
        Filters filters = build.excludeFields("*.c1.missing").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordObject(collectionId, obj, 1002);

        // then
        Struct value = getValue(produced);
        assertThat(value.get(AFTER)).isEqualTo(obj.toJson(WRITER_SETTINGS));
    }

    @Test
    public void shouldExcludeNestedFieldsForReadEvent() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("address", new Document()
                        .append("number", 34L)
                        .append("street", "Claude Debussylaan")
                        .append("city", "Amsterdam"))
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));
        Filters filters = build.excludeFields("*.c1.name,*.c1.active,*.c1.address.number").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordObject(collectionId, obj, 1002);

        // then
        String expected = "{"
                +     "\"_id\" : {\"$oid\" : \"" + objId + "\"},"
                +     "\"phone\" : {\"$numberLong\" : \"123\"},"
                +     "\"address\" : {"
                +         "\"street\" : \"Claude Debussylaan\","
                +         "\"city\" : \"Amsterdam\""
                +     "},"
                +     "\"scores\" : [1.2, 3.4, 5.6]"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(AFTER)).isEqualTo(expected);
    }

    @Test
    public void shouldNotExcludeNestedMissingFieldsForReadEvent() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("address", new Document()
                        .append("number", 34L)
                        .append("street", "Claude Debussylaan")
                        .append("city", "Amsterdam"))
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));
        Filters filters = build.excludeFields("*.c1.address.missing").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordObject(collectionId, obj, 1002);

        // then
        Struct value = getValue(produced);
        assertThat(value.get(AFTER)).isEqualTo(obj.toJson(WRITER_SETTINGS));
    }

    @Test
    public void shouldExcludeFieldsForInsertEvent() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));
        Filters filters = build.excludeFields("*.c1.name,*.c1.active").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createEvent(obj, "i"), 1002);

        // then
        String expected = "{"
                +     "\"_id\" : {\"$oid\" : \"" + objId + "\"},"
                +     "\"phone\" : {\"$numberLong\" : \"123\"},"
                +     "\"scores\" : [1.2, 3.4, 5.6]"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(AFTER)).isEqualTo(expected);
    }

    @Test
    public void shouldNotExcludeMissingFieldsForInsertEvent() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));
        Filters filters = build.excludeFields("*.c1.missing").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createEvent(obj, "i"), 1002);

        // then
        Struct value = getValue(produced);
        assertThat(value.get(AFTER)).isEqualTo(obj.toJson(WRITER_SETTINGS));
    }

    @Test
    public void shouldExcludeNestedFieldsForInsertEvent() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("address", new Document()
                        .append("number", 34L)
                        .append("street", "Claude Debussylaan")
                        .append("city", "Amsterdam"))
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));
        Filters filters = build.excludeFields("*.c1.name,*.c1.active,*.c1.address.number").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createEvent(obj, "i"), 1002);

        // then
        String expected = "{"
                +     "\"_id\" : {\"$oid\" : \"" + objId + "\"},"
                +     "\"phone\" : {\"$numberLong\" : \"123\"},"
                +     "\"address\" : {"
                +         "\"street\" : \"Claude Debussylaan\","
                +         "\"city\" : \"Amsterdam\""
                +     "},"
                +     "\"scores\" : [1.2, 3.4, 5.6]"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(AFTER)).isEqualTo(expected);
    }

    @Test
    public void shouldNotExcludeNestedMissingFieldsForInsertEvent() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("address", new Document()
                        .append("number", 34L)
                        .append("street", "Claude Debussylaan")
                        .append("city", "Amsterdam"))
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));
        Filters filters = build.excludeFields("*.c1.address.missing").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createEvent(obj, "i"), 1002);

        // then
        Struct value = getValue(produced);
        assertThat(value.get(AFTER)).isEqualTo(obj.toJson(WRITER_SETTINGS));
    }

    @Test
    public void shouldExcludeFieldsForUpdateEvent() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));
        Filters filters = build.excludeFields("*.c1.name,*.c1.active").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        String expected = "{"
                +     "\"_id\" : {\"$oid\" : \"" + objId + "\"},"
                +     "\"phone\" : {\"$numberLong\" : \"123\"},"
                +     "\"scores\" : [1.2, 3.4, 5.6]"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(expected);
    }

    @Test
    public void shouldNotExcludeMissingFieldsForUpdateEvent() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));
        Filters filters = build.excludeFields("*.c1.missing").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(obj.toJson(WRITER_SETTINGS));
    }

    @Test
    public void shouldExcludeNestedFieldsForUpdateEventWithEmbeddedDocument() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("address", new Document()
                        .append("number", 34L)
                        .append("street", "Claude Debussylaan")
                        .append("city", "Amsterdam"))
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));
        Filters filters = build.excludeFields("*.c1.name,*.c1.active,*.c1.address.number").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        String expected = "{"
                +     "\"_id\" : {\"$oid\" : \"" + objId + "\"},"
                +     "\"phone\" : {\"$numberLong\" : \"123\"},"
                +     "\"address\" : {"
                +         "\"street\" : \"Claude Debussylaan\","
                +         "\"city\" : \"Amsterdam\""
                +     "},"
                +     "\"scores\" : [1.2, 3.4, 5.6]"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(expected);
    }

    @Test
    public void shouldNotExcludeNestedMissingFieldsForUpdateEventWithEmbeddedDocument() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("address", new Document()
                        .append("number", 34L)
                        .append("street", "Claude Debussylaan")
                        .append("city", "Amsterdam"))
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));
        Filters filters = build.excludeFields("*.c1.address.missing").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(obj.toJson(WRITER_SETTINGS));
    }

    @Test
    public void shouldExcludeNestedFieldsForUpdateEventWithArrayOfEmbeddedDocuments() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("addresses", Arrays.asList(
                        new Document()
                                .append("number", 34L)
                                .append("street", "Claude Debussylaan")
                                .append("city", "Amsterdam"),
                        new Document()
                                .append("number", 7L)
                                .append("street", "Fragkokklisias")
                                .append("city", "Athens")))
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));
        Filters filters = build.excludeFields("*.c1.name,*.c1.addresses.number").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        String expected = "{"
                +     "\"_id\" : {\"$oid\" : \"" + objId + "\"},"
                +     "\"phone\" : {\"$numberLong\" : \"123\"},"
                +     "\"addresses\" : ["
                +         "{"
                +             "\"street\" : \"Claude Debussylaan\","
                +             "\"city\" : \"Amsterdam\""
                +         "}, "
                +         "{"
                +             "\"street\" : \"Fragkokklisias\","
                +             "\"city\" : \"Athens\""
                +         "}"
                +     "],"
                +     "\"active\" : true,"
                +     "\"scores\" : [1.2, 3.4, 5.6]"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(expected);
    }

    @Test
    public void shouldNotExcludeNestedFieldsForUpdateEventWithArrayOfArrays() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("addresses", Arrays.asList(
                        Collections.singletonList(new Document()
                                .append("number", 34L)
                                .append("street", "Claude Debussylaan")
                                .append("city", "Amsterdam")),
                        Collections.singletonList(new Document()
                                .append("number", 7L)
                                .append("street", "Fragkokklisias")
                                .append("city", "Athens"))))
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));
        Filters filters = build.excludeFields("*.c1.name,*.c1.addresses.number").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        String expected = "{"
                +     "\"_id\" : {\"$oid\" : \"" + objId + "\"},"
                +     "\"phone\" : {\"$numberLong\" : \"123\"},"
                +     "\"addresses\" : ["
                +         "["
                +             "{"
                +                 "\"number\" : {\"$numberLong\" : \"34\"},"
                +                 "\"street\" : \"Claude Debussylaan\","
                +                 "\"city\" : \"Amsterdam\""
                +             "}"
                +         "], "
                +         "["
                +             "{"
                +                 "\"number\" : {\"$numberLong\" : \"7\"},"
                +                 "\"street\" : \"Fragkokklisias\","
                +                 "\"city\" : \"Athens\""
                +             "}"
                +         "]"
                +     "],"
                +     "\"active\" : true,"
                +     "\"scores\" : [1.2, 3.4, 5.6]"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(expected);
    }

    @Test
    public void shouldExcludeFieldsForSetTopLevelFieldUpdateEvent() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("phone", 123L));
        Filters filters = build.excludeFields("*.c1.name").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        String expected = "{"
                +     "\"$set\" : {"
                +         "\"phone\" : {\"$numberLong\" : \"123\"}"
                +     "}"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(expected);
    }

    @Test
    public void shouldExcludeFieldsForUnsetTopLevelFieldUpdateEvent() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("$unset", new Document()
                        .append("name", "")
                        .append("phone", ""));
        Filters filters = build.excludeFields("*.c1.name").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        String expected = "{"
                +     "\"$unset\" : {"
                +         "\"phone\" : \"\""
                +     "}"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(expected);
    }

    @Test
    public void shouldExcludeNestedFieldsForSetTopLevelFieldUpdateEventWithEmbeddedDocument() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("phone", 123L)
                        .append("address", new Document()
                                .append("number", 34L)
                                .append("street", "Claude Debussylaan")
                                .append("city", "Amsterdam")));
        Filters filters = build.excludeFields("*.c1.name,*.c1.address.number").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        String expected = "{"
                +     "\"$set\" : {"
                +         "\"phone\" : {\"$numberLong\" : \"123\"},"
                +         "\"address\" : {"
                +             "\"street\" : \"Claude Debussylaan\","
                +             "\"city\" : \"Amsterdam\""
                +         "}"
                +     "}"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(expected);
    }

    @Test
    public void shouldExcludeNestedFieldsForSetTopLevelFieldUpdateEventWithArrayOfEmbeddedDocuments() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("phone", 123L)
                        .append("addresses", Arrays.asList(
                                new Document()
                                        .append("number", 34L)
                                        .append("street", "Claude Debussylaan")
                                        .append("city", "Amsterdam"),
                                new Document()
                                        .append("number", 7L)
                                        .append("street", "Fragkokklisias")
                                        .append("city", "Athens"))));
        Filters filters = build.excludeFields("*.c1.name,*.c1.addresses.number").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        String expected = "{"
                +     "\"$set\" : {"
                +         "\"phone\" : {\"$numberLong\" : \"123\"},"
                +         "\"addresses\" : ["
                +             "{"
                +                 "\"street\" : \"Claude Debussylaan\","
                +                 "\"city\" : \"Amsterdam\""
                +             "}, "
                +             "{"
                +                 "\"street\" : \"Fragkokklisias\","
                +                 "\"city\" : \"Athens\""
                +             "}"
                +         "]"
                +     "}"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(expected);
    }

    @Test
    public void shouldNotExcludeNestedFieldsForSetTopLevelFieldUpdateEventWithArrayOfArrays() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("phone", 123L)
                        .append("addresses", Arrays.asList(
                                Collections.singletonList(new Document()
                                        .append("number", 34L)
                                        .append("street", "Claude Debussylaan")
                                        .append("city", "Amsterdam")),
                                Collections.singletonList(new Document()
                                        .append("number", 7L)
                                        .append("street", "Fragkokklisias")
                                        .append("city", "Athens")))));
        Filters filters = build.excludeFields("*.c1.name,*.c1.addresses.number").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        String expected = "{"
                +     "\"$set\" : {"
                +         "\"phone\" : {\"$numberLong\" : \"123\"},"
                +         "\"addresses\" : ["
                +             "["
                +                 "{"
                +                     "\"number\" : {\"$numberLong\" : \"34\"},"
                +                     "\"street\" : \"Claude Debussylaan\","
                +                     "\"city\" : \"Amsterdam\""
                +                 "}"
                +             "], "
                +             "["
                +                 "{"
                +                     "\"number\" : {\"$numberLong\" : \"7\"},"
                +                     "\"street\" : \"Fragkokklisias\","
                +                     "\"city\" : \"Athens\""
                +                 "}"
                +             "]"
                +         "]"
                +     "}"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(expected);
    }

    @Test
    public void shouldExcludeNestedFieldsForSetNestedFieldUpdateEventWithEmbeddedDocument() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("address.number", 34L)
                        .append("address.street", "Claude Debussylaan")
                        .append("address.city", "Amsterdam"));
        Filters filters = build.excludeFields("*.c1.name,*.c1.address.number").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        String expected = "{"
                +     "\"$set\" : {"
                +         "\"address.street\" : \"Claude Debussylaan\","
                +         "\"address.city\" : \"Amsterdam\""
                +     "}"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(expected);
    }

    @Test
    public void shouldExcludeNestedFieldsForSetNestedFieldUpdateEventWithArrayOfEmbeddedDocuments() throws InterruptedException {
        // source document can have the following structure:
        // {
        //   "name": "Sally",
        //   "addresses": [
        //      {
        //         "number": 34,
        //         "street": "Claude Debussylaan",
        //         "city": "Amsterdam"
        //      }
        //   ]
        // }

        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("addresses.0.number", 34L)
                        .append("addresses.0.street", "Claude Debussylaan")
                        .append("addresses.0.city", "Amsterdam"));
        Filters filters = build.excludeFields("*.c1.addresses.number").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        String expected = "{"
                +     "\"$set\" : {"
                +         "\"name\" : \"Sally\","
                +         "\"addresses.0.street\" : \"Claude Debussylaan\","
                +         "\"addresses.0.city\" : \"Amsterdam\""
                +     "}"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(expected);
    }

    @Test
    public void shouldNotExcludeNestedFieldsForSetNestedFieldUpdateEventWithArrayOfArrays() throws InterruptedException {
        // source document can have the following structure:
        // {
        //   "name": "Sally",
        //   "addresses": [
        //      [
        //         {
        //            "number": 34,
        //            "street": "Claude Debussylaan",
        //            "city": "Amsterdam"
        //         }
        //      ]
        //   ]
        // }

        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("addresses.0.0.number", 34L)
                        .append("addresses.0.0.street", "Claude Debussylaan")
                        .append("addresses.0.0.city", "Amsterdam"));
        Filters filters = build.excludeFields("*.c1.addresses.number").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(obj.toJson(WRITER_SETTINGS));
    }

    @Test
    public void shouldExcludeNestedFieldsForSetNestedFieldUpdateEventWithSeveralArrays() throws InterruptedException {
        // source document can have the following structure:
        // {
        //   "name": "Sally",
        //   "addresses": [
        //      {
        //         "second": [
        //            {
        //               "number": 34,
        //               "street": "Claude Debussylaan",
        //               "city": "Amsterdam"
        //            }
        //         ]
        //      }
        //   ]
        // }

        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("addresses.0.second.0.number", 34L)
                        .append("addresses.0.second.0.street", "Claude Debussylaan")
                        .append("addresses.0.second.0.city", "Amsterdam"));
        Filters filters = build.excludeFields("*.c1.addresses.second.number").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        String expected = "{"
                +     "\"$set\" : {"
                +         "\"name\" : \"Sally\","
                +         "\"addresses.0.second.0.street\" : \"Claude Debussylaan\","
                +         "\"addresses.0.second.0.city\" : \"Amsterdam\""
                +     "}"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(expected);
    }

    @Test
    public void shouldExcludeFieldsForSetNestedFieldUpdateEventWithArrayOfEmbeddedDocuments() throws InterruptedException {
        // source document can have the following structure:
        // {
        //   "name": "Sally",
        //   "addresses": [
        //      {
        //         "number": 34,
        //         "street": "Claude Debussylaan",
        //         "city": "Amsterdam"
        //      }
        //   ]
        // }

        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("addresses.0.number", 34L)
                        .append("addresses.0.street", "Claude Debussylaan")
                        .append("addresses.0.city", "Amsterdam"));
        Filters filters = build.excludeFields("*.c1.addresses").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        String expected = "{"
                +     "\"$set\" : {"
                +         "\"name\" : \"Sally\""
                +     "}"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(expected);
    }

    @Test
    public void shouldExcludeFieldsForSetToArrayFieldUpdateEventWithArrayOfEmbeddedDocuments() throws InterruptedException {
        // source document can have the following structure:
        // {
        //   "name": "Sally",
        //   "addresses": [
        //      {
        //         "number": 34,
        //         "street": "Claude Debussylaan",
        //         "city": "Amsterdam"
        //      }
        //   ]
        // }

        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("addresses.0", new Document()
                                .append("number", 34L)
                                .append("street", "Claude Debussylaan")
                                .append("city", "Amsterdam")));
        Filters filters = build.excludeFields("*.c1.addresses").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        String expected = "{"
                +     "\"$set\" : {"
                +         "\"name\" : \"Sally\""
                +     "}"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(expected);
    }

    @Test
    public void shouldExcludeNestedFieldsForUnsetNestedFieldUpdateEventWithEmbeddedDocument() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("$unset", new Document()
                        .append("name", "")
                        .append("address.number", "")
                        .append("address.street", "")
                        .append("address.city", ""));
        Filters filters = build.excludeFields("*.c1.name,*.c1.address.number").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        String expected = "{"
                +     "\"$unset\" : {"
                +         "\"address.street\" : \"\","
                +         "\"address.city\" : \"\""
                +     "}"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(expected);
    }

    @Test
    public void shouldExcludeNestedFieldsForUnsetNestedFieldUpdateEventWithArrayOfEmbeddedDocuments() throws InterruptedException {
        // source document can have the following structure:
        // {
        //   "name": "Sally",
        //   "addresses": [
        //      {
        //         "number": 34,
        //         "street": "Claude Debussylaan",
        //         "city": "Amsterdam"
        //      }
        //   ]
        // }

        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("$unset", new Document()
                        .append("name", "")
                        .append("addresses.0.number", "")
                        .append("addresses.0.street", "")
                        .append("addresses.0.city", ""));
        Filters filters = build.excludeFields("*.c1.addresses.number").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        String expected = "{"
                +     "\"$unset\" : {"
                +         "\"name\" : \"\","
                +         "\"addresses.0.street\" : \"\","
                +         "\"addresses.0.city\" : \"\""
                +     "}"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(expected);
    }

    @Test
    public void shouldNotExcludeNestedFieldsForUnsetNestedFieldUpdateEventWithArrayOfArrays() throws InterruptedException {
        // source document can have the following structure:
        // {
        //   "name": "Sally",
        //   "addresses": [
        //      [
        //         {
        //            "number": 34,
        //            "street": "Claude Debussylaan",
        //            "city": "Amsterdam"
        //         }
        //      ]
        //   ]
        // }

        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("$unset", new Document()
                        .append("name", "")
                        .append("addresses.0.0.number", "")
                        .append("addresses.0.0.street", "")
                        .append("addresses.0.0.city", ""));
        Filters filters = build.excludeFields("*.c1.addresses.number").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(obj.toJson(WRITER_SETTINGS));
    }

    @Test
    public void shouldExcludeNestedFieldsForUnsetNestedFieldUpdateEventWithSeveralArrays() throws InterruptedException {
        // source document can have the following structure:
        // {
        //   "name": "Sally",
        //   "addresses": [
        //      {
        //         "second": [
        //            {
        //               "number": 34,
        //               "street": "Claude Debussylaan",
        //               "city": "Amsterdam"
        //            }
        //         ]
        //      }
        //   ]
        // }

        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("$unset", new Document()
                        .append("name", "")
                        .append("addresses.0.second.0.number", "")
                        .append("addresses.0.second.0.street", "")
                        .append("addresses.0.second.0.city", ""));
        Filters filters = build.excludeFields("*.c1.addresses.second.number").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        String expected = "{"
                +     "\"$unset\" : {"
                +         "\"name\" : \"\","
                +         "\"addresses.0.second.0.street\" : \"\","
                +         "\"addresses.0.second.0.city\" : \"\""
                +     "}"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(expected);
    }

    @Test
    public void shouldExcludeFieldsForUnsetNestedFieldUpdateEventWithArrayOfEmbeddedDocuments() throws InterruptedException {
        // source document can have the following structure:
        // {
        //   "name": "Sally",
        //   "addresses": [
        //      {
        //         "number": 34,
        //         "street": "Claude Debussylaan",
        //         "city": "Amsterdam"
        //      }
        //   ]
        // }

        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("$unset", new Document()
                        .append("name", "")
                        .append("addresses.0.number", "")
                        .append("addresses.0.street", "")
                        .append("addresses.0.city", ""));
        Filters filters = build.excludeFields("*.c1.addresses").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createUpdateEvent(obj, objId), 1002);

        // then
        String expected = "{"
                +     "\"$unset\" : {"
                +         "\"name\" : \"\""
                +     "}"
                + "}";
        Struct value = getValue(produced);
        assertThat(value.get(PATCH)).isEqualTo(expected);
    }

    @Test
    public void shouldExcludeFieldsForDeleteEvent() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document("_id", objId);
        Filters filters = build.excludeFields("*.c1.name,*.c1.active").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createEvent(obj, "d"), 1002);

        // then
        Struct value = getValue(produced);
        String json = value.getString(AFTER);
        if (json == null) {
            json = value.getString(PATCH);
        }
        assertThat(json).isNull();
    }

    @Test
    public void shouldExcludeFieldsForDeleteTombstoneEvent() throws InterruptedException {
        // given
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        ObjectId objId = new ObjectId();
        Document obj = new Document("_id", objId);
        Filters filters = build.excludeFields("*.c1.name,*.c1.active").createFilters();
        List<SourceRecord> produced = new ArrayList<>();
        RecordMakers recordMakers = new RecordMakers(filters, source, topicSelector, produced::add, true);

        // when
        recordMakers.forCollection(collectionId).recordEvent(createEvent(obj, "d"), 1002);

        // then
        SourceRecord record = produced.get(1);
        assertThat(record.value()).isNull();
    }

    private Struct getValue(List<SourceRecord> produced) {
        SourceRecord record = produced.get(0);
        return (Struct) record.value();
    }

    private Document createEvent(Document obj, String op) {
        BsonTimestamp ts = new BsonTimestamp(1000, 1);
        return new Document()
                .append("o", obj)
                .append("ns", "dbA.c1")
                .append("ts", ts)
                .append("h", 12345678L)
                .append("op", op);
    }

    private Document createUpdateEvent(Document obj, ObjectId objId) {
        BsonTimestamp ts = new BsonTimestamp(1000, 1);
        return new Document()
                .append("o", obj)
                .append("o2", objId)
                .append("ns", "dbA.c1")
                .append("ts", ts)
                .append("h", 12345678L)
                .append("op", "u");
    }
}
