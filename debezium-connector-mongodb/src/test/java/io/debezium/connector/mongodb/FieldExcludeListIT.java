/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.connector.mongodb.MongoDbSchema.COMPACT_JSON_SETTINGS;
import static io.debezium.data.Envelope.FieldName.AFTER;
import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.Fail.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneOptions;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.ConnectionContext.MongoPrimary;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

// todo: extend AbstractMongoConnectorIT?
public class FieldExcludeListIT extends AbstractConnectorTest {

    private static final String SERVER_NAME = "serverX";
    private static final String PATCH = "patch";

    private Configuration config;
    private MongoDbTaskContext context;

    @Before
    public void beforeEach() {
        Debug.disable();
        Print.disable();
        stopConnector();
        initializeConnectorTestFramework();
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            if (context != null) {
                context.getConnectionContext().shutdown();
            }
        }
    }

    @Test
    public void shouldNotExcludeFieldsForEventOfOtherCollection() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        assertReadRecord("*.c2.name,*.c2.active", obj, AFTER, obj.toJson(COMPACT_JSON_SETTINGS));
    }

    @Test
    public void shouldExcludeFieldsForReadEvent() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        // @formatter:off
        String expected = "{"
                +     "\"_id\": {\"$oid\": \"" + objId + "\"},"
                +     "\"phone\": {\"$numberLong\": \"123\"},"
                +     "\"scores\": [1.2,3.4,5.6]"
                + "}";
        // @formatter:on

        assertReadRecord("*.c1.name,*.c1.active", obj, AFTER, expected);
    }

    @Test
    public void shouldNotExcludeMissingFieldsForReadEvent() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        assertReadRecord("*.c1.missing", obj, AFTER, obj.toJson(COMPACT_JSON_SETTINGS));
    }

    @Test
    public void shouldExcludeNestedFieldsForReadEvent() throws InterruptedException {
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

        // @formatter:off
        String expected = "{"
                +     "\"_id\": {\"$oid\": \"" + objId + "\"},"
                +     "\"phone\": {\"$numberLong\": \"123\"},"
                +     "\"address\": {"
                +         "\"street\": \"Claude Debussylaan\","
                +         "\"city\": \"Amsterdam\""
                +     "},"
                +     "\"scores\": [1.2,3.4,5.6]"
                + "}";
        // @formatter:on

        assertReadRecord("*.c1.name,*.c1.active,*.c1.address.number", obj, AFTER, expected);
    }

    @Test
    public void shouldNotExcludeNestedMissingFieldsForReadEvent() throws InterruptedException {
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

        assertReadRecord("*.c1.address.missing", obj, AFTER, obj.toJson(COMPACT_JSON_SETTINGS));
    }

    @Test
    public void shouldExcludeFieldsForInsertEvent() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        // @formatter:off
        String expected = "{"
                +     "\"_id\": {\"$oid\": \"" + objId + "\"},"
                +     "\"phone\": {\"$numberLong\": \"123\"},"
                +     "\"scores\": [1.2,3.4,5.6]"
                + "}";
        // @formatter:on

        assertInsertRecord("*.c1.name,*.c1.active", obj, AFTER, expected);
    }

    @Test
    public void shouldNotExcludeMissingFieldsForInsertEvent() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        assertInsertRecord("*.c1.missing", obj, AFTER, obj.toJson(COMPACT_JSON_SETTINGS));
    }

    @Test
    public void shouldExcludeNestedFieldsForInsertEvent() throws InterruptedException {
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

        // @formatter:off
        String expected = "{"
                +     "\"_id\": {\"$oid\": \"" + objId + "\"},"
                +     "\"phone\": {\"$numberLong\": \"123\"},"
                +     "\"address\": {"
                +         "\"street\": \"Claude Debussylaan\","
                +         "\"city\": \"Amsterdam\""
                +     "},"
                +     "\"scores\": [1.2,3.4,5.6]"
                + "}";
        // @formatter:on

        assertInsertRecord("*.c1.name,*.c1.active,*.c1.address.number", obj, AFTER, expected);
    }

    @Test
    public void shouldNotExcludeNestedMissingFieldsForInsertEvent() throws InterruptedException {
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

        assertInsertRecord("*.c1.address.missing", obj, AFTER, obj.toJson(COMPACT_JSON_SETTINGS));
    }

    @Test
    public void shouldExcludeFieldsForUpdateEvent() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 456L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6, 7.8));

        Document updateObj = new Document()
                .append("phone", 123L)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +          "\"phone\": {\"$numberLong\": \"123\"},"
                +          "\"scores\": [1.2,3.4,5.6]"
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name,*.c1.active", objId, obj, updateObj, PATCH, expected);
    }

    @Test
    public void shouldNotExcludeMissingFieldsForUpdateEvent() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 456L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6, 7.8));

        Document updateObj = new Document()
                .append("phone", 123L)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +          "\"phone\": {\"$numberLong\": \"123\"},"
                +          "\"scores\": [1.2,3.4,5.6]"
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.missing", objId, obj, updateObj, PATCH, expected);
    }

    @Test
    public void shouldExcludeNestedFieldsForUpdateEventWithEmbeddedDocument() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 456L)
                .append("address", new Document()
                        .append("number", 35L)
                        .append("street", "Claude Debussylaane")
                        .append("city", "Amsterdame"))
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6, 7.8));

        Document updateObj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("address", new Document()
                        .append("number", 34L)
                        .append("street", "Claude Debussylaan")
                        .append("city", "Amsterdam"))
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +          "\"address\": {"
                +              "\"street\": \"Claude Debussylaan\","
                +              "\"city\": \"Amsterdam\""
                +          "},"
                +          "\"phone\": {\"$numberLong\": \"123\"},"
                +          "\"scores\": [1.2,3.4,5.6]"
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name,*.c1.active,*.c1.address.number", objId, obj, updateObj, PATCH, expected);
    }

    @Test
    public void shouldNotExcludeNestedMissingFieldsForUpdateEventWithEmbeddedDocument() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 456L)
                .append("address", new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame"))
                .append("active", false)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6, 7.8));

        Document updateObj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("address", new Document()
                        .append("number", 34L)
                        .append("street", "Claude Debussylaan")
                        .append("city", "Amsterdam"))
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +          "\"active\": true,"
                +          "\"address\": {"
                +              "\"number\": {\"$numberLong\": \"34\"},"
                +              "\"street\": \"Claude Debussylaan\","
                +              "\"city\": \"Amsterdam\""
                +          "},"
                +          "\"phone\": {\"$numberLong\": \"123\"},"
                +          "\"scores\": [1.2,3.4,5.6]"
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.address.missing", objId, obj, updateObj, PATCH, expected);
    }

    @Test
    public void shouldExcludeNestedFieldsForUpdateEventWithArrayOfEmbeddedDocuments() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 456L)
                .append("addresses", Arrays.asList(
                        new Document()
                                .append("number", 45L)
                                .append("street", "Claude Debussylaann")
                                .append("city", "Amsterdame"),
                        new Document()
                                .append("number", 8L)
                                .append("street", "Fragkokklisiass")
                                .append("city", "Athense")))
                .append("active", false)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6, 7.8));

        Document updateObj = new Document()
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

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +          "\"active\": true,"
                +          "\"addresses\": ["
                +              "{"
                +                  "\"street\": \"Claude Debussylaan\","
                +                  "\"city\": \"Amsterdam\""
                +              "},"
                +              "{"
                +                  "\"street\": \"Fragkokklisias\","
                +                  "\"city\": \"Athens\""
                +              "}"
                +          "],"
                +          "\"phone\": {\"$numberLong\": \"123\"},"
                +          "\"scores\": [1.2,3.4,5.6]"
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name,*.c1.addresses.number", objId, obj, updateObj, PATCH, expected);
    }

    @Test
    public void shouldNotExcludeNestedFieldsForUpdateEventWithArrayOfArrays() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally Mae")
                .append("phone", 456L)
                .append("addresses", Arrays.asList(
                        Collections.singletonList(new Document()
                                .append("number", 45L)
                                .append("street", "Claude Debussylaann")
                                .append("city", "Amsterdame")),
                        Collections.singletonList(new Document()
                                .append("number", 8L)
                                .append("street", "Fragkokklisiass")
                                .append("city", "Athenss"))))
                .append("active", false)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6, 7.8));

        Document updateObj = new Document()
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

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +          "\"active\": true,"
                +          "\"addresses\": ["
                +              "["
                +                  "{"
                +                      "\"number\": {\"$numberLong\": \"34\"},"
                +                      "\"street\": \"Claude Debussylaan\","
                +                      "\"city\": \"Amsterdam\""
                +                  "}"
                +              "],"
                +              "["
                +                  "{"
                +                      "\"number\": {\"$numberLong\": \"7\"},"
                +                      "\"street\": \"Fragkokklisias\","
                +                      "\"city\": \"Athens\""
                +                  "}"
                +              "]"
                +          "],"
                +          "\"phone\": {\"$numberLong\": \"123\"},"
                +          "\"scores\": [1.2,3.4,5.6]"
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name,*.c1.addresses.number", objId, obj, updateObj, PATCH, expected);
    }

    @Test
    public void shouldExcludeFieldsForSetTopLevelFieldUpdateEvent() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("phone", 456L);

        Document updateObj = new Document()
                .append("name", "Sally")
                .append("phone", 123L);

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"phone\": {\"$numberLong\": \"123\"}"
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name", objId, obj, updateObj, PATCH, expected);
    }

    @Test
    public void shouldExcludeFieldsForUnsetTopLevelFieldUpdateEvent() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 456L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        Document updateObj = new Document()
                .append("name", "")
                .append("phone", "");

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$unset\": {"
                +          "\"phone\": true"
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name", objId, obj, updateObj, false, PATCH, expected);
    }

    @Test
    public void shouldExcludeNestedFieldsForSetTopLevelFieldUpdateEventWithEmbeddedDocument() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("phone", 456L)
                .append("address", new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame"));

        Document updateObj = new Document()
                .append("name", "Sally")
                .append("phone", 123L)
                .append("address", new Document()
                        .append("number", 34L)
                        .append("street", "Claude Debussylaan")
                        .append("city", "Amsterdam"));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"address\": {"
                +             "\"street\": \"Claude Debussylaan\","
                +             "\"city\": \"Amsterdam\""
                +         "},"
                +         "\"phone\": {\"$numberLong\": \"123\"}"
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name,*.c1.address.number", objId, obj, updateObj, PATCH, expected);
    }

    @Test
    public void shouldExcludeNestedFieldsForSetTopLevelFieldUpdateEventWithArrayOfEmbeddedDocuments() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("phone", 456L)
                .append("addresses", Arrays.asList(
                        new Document()
                                .append("number", 45L)
                                .append("street", "Claude Debussylaann")
                                .append("city", "Amsterdame"),
                        new Document()
                                .append("number", 8L)
                                .append("street", "Fragkokklisiass")
                                .append("city", "Athense")));

        Document updateObj = new Document()
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
                                .append("city", "Athens")));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"addresses\": ["
                +             "{"
                +                 "\"street\": \"Claude Debussylaan\","
                +                 "\"city\": \"Amsterdam\""
                +             "},"
                +             "{"
                +                 "\"street\": \"Fragkokklisias\","
                +                 "\"city\": \"Athens\""
                +             "}"
                +         "],"
                +         "\"phone\": {\"$numberLong\": \"123\"}"
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name,*.c1.addresses.number", objId, obj, updateObj, PATCH, expected);
    }

    @Test
    public void shouldNotExcludeNestedFieldsForSetTopLevelFieldUpdateEventWithArrayOfArrays() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("phone", 456L)
                .append("addresses", Arrays.asList(
                        Collections.singletonList(new Document()
                                .append("number", 45L)
                                .append("street", "Claude Debussylaann")
                                .append("city", "Amsterdame")),
                        Collections.singletonList(new Document()
                                .append("number", 8L)
                                .append("street", "Fragkokklisiass")
                                .append("city", "Athense"))));

        Document updateObj = new Document()
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
                                .append("city", "Athens"))));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"addresses\": ["
                +             "["
                +                 "{"
                +                     "\"number\": {\"$numberLong\": \"34\"},"
                +                     "\"street\": \"Claude Debussylaan\","
                +                     "\"city\": \"Amsterdam\""
                +                 "}"
                +             "],"
                +             "["
                +                 "{"
                +                     "\"number\": {\"$numberLong\": \"7\"},"
                +                     "\"street\": \"Fragkokklisias\","
                +                     "\"city\": \"Athens\""
                +                 "}"
                +             "]"
                +         "],"
                +         "\"phone\": {\"$numberLong\": \"123\"}"
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name,*.c1.addresses.number", objId, obj, updateObj, PATCH, expected);
    }

    @Test
    public void shouldExcludeNestedFieldsForSetNestedFieldUpdateEventWithEmbeddedDocument() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("phone", 456L)
                .append("address", new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame"));

        Document updateObj = new Document()
                .append("name", "Sally")
                .append("address.number", 34L)
                .append("address.street", "Claude Debussylaan")
                .append("address.city", "Amsterdam");

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"address.city\": \"Amsterdam\","
                +         "\"address.street\": \"Claude Debussylaan\""
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name,*.c1.address.number", objId, obj, updateObj, PATCH, expected);
    }

    @Test
    public void shouldExcludeNestedFieldsForSetNestedFieldUpdateEventWithArrayOfEmbeddedDocuments() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("addresses", Arrays.asList(
                        new Document()
                                .append("number", 45L)
                                .append("street", "Claude Debussylaann")
                                .append("city", "Amsterdame")));

        Document updateObj = new Document()
                .append("name", "Sally")
                .append("addresses.0.number", 34L)
                .append("addresses.0.street", "Claude Debussylaan")
                .append("addresses.0.city", "Amsterdam");

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"addresses.0.city\": \"Amsterdam\","
                +         "\"addresses.0.street\": \"Claude Debussylaan\","
                +         "\"name\": \"Sally\""
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.addresses.number", objId, obj, updateObj, PATCH, expected);
    }

    @Test
    public void shouldNotExcludeNestedFieldsForSetNestedFieldUpdateEventWithArrayOfArrays() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("addresses", Arrays.asList(
                        Collections.singletonList(new Document()
                                .append("number", 45L)
                                .append("street", "Claude Debussylaann")
                                .append("city", "Amsterdame")),
                        Collections.singletonList(new Document()
                                .append("number", 8L)
                                .append("street", "Fragkokklisiass")
                                .append("city", "Athense"))));

        Document updateObj = new Document()
                .append("name", "Sally")
                .append("addresses.0.0.number", 34L)
                .append("addresses.0.0.street", "Claude Debussylaan")
                .append("addresses.0.0.city", "Amsterdam");

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"addresses.0.0.city\": \"Amsterdam\","
                +         "\"addresses.0.0.number\": {\"$numberLong\": \"34\"},"
                +         "\"addresses.0.0.street\": \"Claude Debussylaan\","
                +         "\"name\": \"Sally\""
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.addresses.number", objId, obj, updateObj, PATCH, expected);
    }

    @Test
    public void shouldExcludeNestedFieldsForSetNestedFieldUpdateEventWithSeveralArrays() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("addresses", Arrays.asList(Collections.singletonMap("second",
                        Arrays.asList(
                                new Document()
                                        .append("number", 45L)
                                        .append("street", "Claude Debussylaann")
                                        .append("city", "Amsterdame")))));
        Document updateObj = new Document()
                .append("name", "Sally")
                .append("addresses.0.second.0.number", 34L)
                .append("addresses.0.second.0.street", "Claude Debussylaan")
                .append("addresses.0.second.0.city", "Amsterdam");

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"addresses.0.second.0.city\": \"Amsterdam\","
                +         "\"addresses.0.second.0.street\": \"Claude Debussylaan\","
                +         "\"name\": \"Sally\""
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.addresses.second.number", objId, obj, updateObj, PATCH, expected);
    }

    @Test
    public void shouldExcludeFieldsForSetNestedFieldUpdateEventWithArrayOfEmbeddedDocuments() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("addresses", Arrays.asList(
                        new Document()
                                .append("number", 45L)
                                .append("street", "Claude Debussylaann")
                                .append("city", "Amsterdame")));

        Document updateObj = new Document()
                .append("name", "Sally")
                .append("addresses.0.0.number", 34L)
                .append("addresses.0.0.street", "Claude Debussylaan")
                .append("addresses.0.0.city", "Amsterdam");

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"name\": \"Sally\""
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.addresses", objId, obj, updateObj, PATCH, expected);
    }

    @Test
    public void shouldExcludeFieldsForSetToArrayFieldUpdateEventWithArrayOfEmbeddedDocuments() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("addresses", Arrays.asList(
                        new Document()
                                .append("number", 45L)
                                .append("street", "Claude Debussylaann")
                                .append("city", "Amsterdame")));

        Document updateObj = new Document()
                .append("name", "Sally")
                .append("addresses.0", new Document()
                        .append("number", 34L)
                        .append("street", "Claude Debussylaan")
                        .append("city", "Amsterdam"));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"name\": \"Sally\""
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.addresses", objId, obj, updateObj, PATCH, expected);
    }

    @Test
    public void shouldExcludeNestedFieldsForUnsetNestedFieldUpdateEventWithEmbeddedDocument() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 456L)
                .append("address", new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame"))
                .append("active", false)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6, 7.8));

        Document updateObj = new Document()
                .append("name", "")
                .append("address.number", "")
                .append("address.street", "")
                .append("address.city", "");

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$unset\": {"
                +         "\"address.city\": true,"
                +         "\"address.street\": true"
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name,*.c1.address.number", objId, obj, updateObj, false, PATCH, expected);
    }

    @Test
    public void shouldExcludeNestedFieldsForUnsetNestedFieldUpdateEventWithArrayOfEmbeddedDocuments() throws InterruptedException {
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

        Document updateObj = new Document()
                .append("name", "")
                .append("addresses.0.number", "")
                .append("addresses.0.street", "")
                .append("addresses.0.city", "");

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$unset\": {"
                +         "\"addresses.0.city\": true,"
                +         "\"addresses.0.street\": true,"
                +         "\"name\": true"
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.addresses.number", objId, obj, updateObj, false, PATCH, expected);
    }

    @Test
    public void shouldNotExcludeNestedFieldsForUnsetNestedFieldUpdateEventWithArrayOfArrays() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("addresses", Arrays.asList(
                        Arrays.asList(
                                new Document()
                                        .append("number", 34L)
                                        .append("street", "Claude Debussylaan")
                                        .append("city", "Amsterdam"))))
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        Document updateObj = new Document()
                .append("name", "")
                .append("addresses.0.0.number", "")
                .append("addresses.0.0.street", "")
                .append("addresses.0.0.city", "");

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$unset\": {"
                +         "\"addresses.0.0.city\": true,"
                +         "\"addresses.0.0.number\": true,"
                +         "\"addresses.0.0.street\": true,"
                +         "\"name\": true"
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.addresses.number", objId, obj, updateObj, false, PATCH, expected);
    }

    @Test
    public void shouldExcludeNestedFieldsForUnsetNestedFieldUpdateEventWithSeveralArrays() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("addresses", Arrays.asList(Collections.singletonMap("second",
                        Arrays.asList(
                                new Document()
                                        .append("number", 34L)
                                        .append("street", "Claude Debussylaan")
                                        .append("city", "Amsterdam")))));

        Document updateObj = new Document()
                .append("name", "")
                .append("addresses.0.second.0.number", "")
                .append("addresses.0.second.0.street", "")
                .append("addresses.0.second.0.city", "");

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$unset\": {"
                +         "\"addresses.0.second.0.city\": true,"
                +         "\"addresses.0.second.0.street\": true,"
                +         "\"name\": true"
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.addresses.second.number", objId, obj, updateObj, false, PATCH, expected);
    }

    @Test
    public void shouldExcludeFieldsForUnsetNestedFieldUpdateEventWithArrayOfEmbeddedDocuments() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("addresses", Arrays.asList(
                        new Document()
                                .append("number", 34L)
                                .append("street", "Claude Debussylaan")
                                .append("city", "Amsterdam")));

        Document updateObj = new Document()
                .append("name", "")
                .append("addresses.0.number", "")
                .append("addresses.0.street", "")
                .append("addresses.0.city", "");

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$unset\": {"
                +         "\"name\": true"
                +     "}"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.addresses", objId, obj, updateObj, false, PATCH, expected);
    }

    @Test
    public void shouldExcludeFieldsForDeleteEvent() throws InterruptedException {
        config = getConfiguration("*.c1.name,*.c1.active");
        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(primary(), "dbA");

        ObjectId objId = new ObjectId();
        Document obj = new Document("_id", objId);
        storeDocuments("dbA", "c1", obj);

        start(MongoDbConnector.class, config);

        SourceRecords snapshotRecords = consumeRecordsByTopic(1);
        assertThat(snapshotRecords.topics().size()).isEqualTo(1);
        assertThat(snapshotRecords.allRecordsInOrder().size()).isEqualTo(1);

        // Wait for streaming to start and perform an update
        waitForStreamingRunning("mongodb", SERVER_NAME);
        deleteDocuments("dbA", "c1", objId);

        // Get the delete records (1 delete and 1 tombstone)
        SourceRecords deleteRecords = consumeRecordsByTopic(2);
        assertThat(deleteRecords.topics().size()).isEqualTo(1);
        assertThat(deleteRecords.allRecordsInOrder().size()).isEqualTo(2);

        // Only validating delete record, non-tombstone
        SourceRecord record = deleteRecords.allRecordsInOrder().get(0);
        Struct value = getValue(record);

        String json = value.getString(AFTER);
        if (json == null) {
            json = value.getString(PATCH);
        }

        assertThat(json).isNull();
    }

    @Test
    public void shouldExcludeFieldsForDeleteTombstoneEvent() throws InterruptedException {
        config = getConfiguration("*.c1.name,*.c1.active");
        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(primary(), "dbA");

        ObjectId objId = new ObjectId();
        Document obj = new Document("_id", objId);
        storeDocuments("dbA", "c1", obj);

        start(MongoDbConnector.class, config);

        SourceRecords snapshotRecords = consumeRecordsByTopic(1);
        assertThat(snapshotRecords.topics().size()).isEqualTo(1);
        assertThat(snapshotRecords.allRecordsInOrder().size()).isEqualTo(1);

        // Wait for streaming to start and perform an update
        waitForStreamingRunning("mongodb", SERVER_NAME);
        deleteDocuments("dbA", "c1", objId);

        // Get the delete records (1 delete and 1 tombstone)
        SourceRecords deleteRecords = consumeRecordsByTopic(2);
        assertThat(deleteRecords.topics().size()).isEqualTo(1);
        assertThat(deleteRecords.allRecordsInOrder().size()).isEqualTo(2);

        // Only validating tombstone record, non-delete
        SourceRecord record = deleteRecords.allRecordsInOrder().get(1);
        Struct value = getValue(record);

        assertThat(value).isNull();
    }

    private Configuration getConfiguration(String blackList) {
        return TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.FIELD_EXCLUDE_LIST, blackList)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbA.c1")
                .with(MongoDbConnectorConfig.LOGICAL_NAME, SERVER_NAME)
                .build();
    }

    private Struct getValue(SourceRecord record) {
        return (Struct) record.value();
    }

    private BiConsumer<String, Throwable> connectionErrorHandler(int numErrorsBeforeFailing) {
        AtomicInteger attempts = new AtomicInteger();
        return (desc, error) -> {
            if (attempts.incrementAndGet() > numErrorsBeforeFailing) {
                fail("Unable to connect to primary after " + numErrorsBeforeFailing + " errors trying to " + desc + ": " + error);
            }
            logger.error("Error while attempting to {}: {}", desc, error.getMessage(), error);
        };
    }

    private MongoPrimary primary() {
        ReplicaSet replicaSet = ReplicaSet.parse(context.getConnectionContext().hosts());
        return context.getConnectionContext().primaryFor(replicaSet, context.filters(), connectionErrorHandler(3));
    }

    private void storeDocuments(String dbName, String collectionName, Document... documents) {
        primary().execute("store documents", mongo -> {
            Testing.debug("Storing in '" + dbName + "." + collectionName + "' document");
            MongoDatabase db = mongo.getDatabase(dbName);
            MongoCollection<Document> coll = db.getCollection(collectionName);
            coll.drop();

            for (Document document : documents) {
                InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
                assertThat(document).isNotNull();
                assertThat(document.size()).isGreaterThan(0);
                coll.insertOne(document, insertOptions);
            }
        });
    }

    private void updateDocuments(String dbName, String collectionName, ObjectId objId, Document document, boolean doSet) {
        primary().execute("update", mongo -> {
            MongoDatabase db = mongo.getDatabase(dbName);
            MongoCollection<Document> coll = db.getCollection(collectionName);
            Document filter = Document.parse("{\"_id\": {\"$oid\": \"" + objId + "\"}}");
            coll.updateOne(filter, new Document().append(doSet ? "$set" : "$unset", document));
        });
    }

    private void deleteDocuments(String dbName, String collectionName, ObjectId objId) {
        primary().execute("delete", mongo -> {
            MongoDatabase db = mongo.getDatabase(dbName);
            MongoCollection<Document> coll = db.getCollection(collectionName);
            Document filter = Document.parse("{\"_id\": {\"$oid\": \"" + objId + "\"}}");
            coll.deleteOne(filter);
        });
    }

    private void assertReadRecord(String blackList, Document snapshotRecord, String field, String expected) throws InterruptedException {
        config = getConfiguration(blackList);
        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(primary(), "dbA");
        storeDocuments("dbA", "c1", snapshotRecord);

        start(MongoDbConnector.class, config);

        SourceRecords snapshotRecords = consumeRecordsByTopic(1);
        assertThat(snapshotRecords.topics().size()).isEqualTo(1);
        assertThat(snapshotRecords.allRecordsInOrder().size()).isEqualTo(1);

        SourceRecord record = snapshotRecords.allRecordsInOrder().get(0);
        Struct value = getValue(record);

        assertThat(value.get(field)).isEqualTo(expected);
    }

    private void assertInsertRecord(String blackList, Document insertRecord, String field, String expected) throws InterruptedException {
        config = getConfiguration(blackList);
        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(primary(), "dbA");

        start(MongoDbConnector.class, config);
        waitForSnapshotToBeCompleted("mongodb", SERVER_NAME);

        storeDocuments("dbA", "c1", insertRecord);

        // Get the insert records
        SourceRecords insertRecords = consumeRecordsByTopic(1);
        assertThat(insertRecords.topics().size()).isEqualTo(1);
        assertThat(insertRecords.allRecordsInOrder().size()).isEqualTo(1);

        SourceRecord record = insertRecords.allRecordsInOrder().get(0);
        Struct value = getValue(record);

        assertThat(value.get(field)).isEqualTo(expected);
    }

    private void assertUpdateRecord(String blackList, ObjectId objectId, Document snapshotRecord, Document updateRecord,
                                    String field, String expected)
            throws InterruptedException {
        assertUpdateRecord(blackList, objectId, snapshotRecord, updateRecord, true, field, expected);
    }

    private void assertUpdateRecord(String blackList, ObjectId objectId, Document snapshotRecord, Document updateRecord,
                                    boolean doSet, String field, String expected)
            throws InterruptedException {
        config = getConfiguration(blackList);
        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(primary(), "dbA");

        storeDocuments("dbA", "c1", snapshotRecord);

        start(MongoDbConnector.class, config);

        // Get the snapshot records
        SourceRecords snapshotRecords = consumeRecordsByTopic(1);
        assertThat(snapshotRecords.topics().size()).isEqualTo(1);
        assertThat(snapshotRecords.allRecordsInOrder().size()).isEqualTo(1);

        // Wait for streaming to start and perform an update
        waitForStreamingRunning("mongodb", SERVER_NAME);
        updateDocuments("dbA", "c1", objectId, updateRecord, doSet);

        // Get the update records
        SourceRecords updateRecords = consumeRecordsByTopic(1);
        assertThat(updateRecords.topics().size()).isEqualTo(1);
        assertThat(updateRecords.allRecordsInOrder().size()).isEqualTo(1);

        SourceRecord record = updateRecords.allRecordsInOrder().get(0);
        Struct value = getValue(record);

        Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(field));
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }
}
