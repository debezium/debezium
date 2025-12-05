/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.connector.mongodb.JsonSerialization.COMPACT_JSON_SETTINGS;
import static io.debezium.data.Envelope.FieldName.AFTER;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Test;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneOptions;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.util.Testing;

public class FieldBlacklistIT extends AbstractMongoConnectorIT {

    private static final String SERVER_NAME = "serverX";

    public static class ExpectedUpdate {

        public final String patch;
        public final String full;
        public final String updatedFields;
        public final List<String> removedFields;

        public ExpectedUpdate(String patch, String full, String updatedFields, List<String> removedFields) {
            super();
            this.patch = patch;
            this.full = full;
            this.updatedFields = updatedFields;
            this.removedFields = removedFields;
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
    public void shouldExcludeFiledWhenParentIsRemoved() throws InterruptedException {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Bob")
                .append("contact", new Document("email", "thebob@example.com"));

        Document updateObj = new Document("contact", "");

        var full = "{\"_id\": {\"$oid\": \"<OID>\"},\"name\": \"Bob\"}";
        var expectedUpdate = new ExpectedUpdate(null, full, "{}", null);
        assertUpdateRecord("*.c1.contact.email", objId, obj, updateObj, false, updateField(), expectedUpdate);
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
        String patch = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +          "\"phone\": {\"$numberLong\": \"123\"},"
                +          "\"scores\": [1.2,3.4,5.6]"
                +     "}"
                + "}";
        String full = "{\"_id\": {\"$oid\": \"<OID>\"}, \"phone\": {\"$numberLong\": \"123\"}, \"scores\": [1.2, 3.4, 5.6]}";
        final String updated = "{\"phone\": 123, \"scores\": [1.2, 3.4, 5.6]}";
        // @formatter:on

        assertUpdateRecord("*.c1.name,*.c1.active", objId, obj, updateObj, updateField(),
                new ExpectedUpdate(patch, full, updated, null));
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
        String patch = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +          "\"phone\": {\"$numberLong\": \"123\"},"
                +          "\"scores\": [1.2,3.4,5.6]"
                +     "}"
                + "}";
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"name\": \"Sally\", "
                +    "\"phone\": {\"$numberLong\": \"123\"}, "
                +    "\"active\": true, "
                +    "\"scores\": [1.2, 3.4, 5.6]"
                + "}";
        final String updated = "{"
                +             "\"phone\": 123, "
                +             "\"scores\": [1.2, 3.4, 5.6]"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.missing", objId, obj, updateObj, updateField(),
                new ExpectedUpdate(patch, full, updated, null));
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
        String patch = "{"
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
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"phone\": {\"$numberLong\": \"123\"}, "
                +          "\"address\": {"
                +              "\"street\": \"Claude Debussylaan\", "
                +              "\"city\": \"Amsterdam\""
                +          "},"
                +    "\"scores\": [1.2, 3.4, 5.6]"
                + "}";
        final String updated = "{"
                +             "\"address\": {"
                +                 "\"street\": \"Claude Debussylaan\", "
                +                 "\"city\": \"Amsterdam\""
                +             "}, "
                +             "\"phone\": 123, "
                +             "\"scores\": [1.2, 3.4, 5.6]"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name,*.c1.active,*.c1.address.number", objId, obj, updateObj, updateField(),
                new ExpectedUpdate(patch, full, updated, null));
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
        String patch = "{"
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
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"name\": \"Sally\", "
                +    "\"phone\": {\"$numberLong\": \"123\"}, "
                +    "\"address\": {"
                +          "\"number\": {\"$numberLong\": \"34\"}, "
                +          "\"street\": \"Claude Debussylaan\", "
                +          "\"city\": \"Amsterdam\""
                +    "},"
                +    "\"active\": true, "
                +    "\"scores\": [1.2, 3.4, 5.6]"
                + "}";
        final String updated = "{"
                +             "\"active\": true, "
                +             "\"address\": {"
                +                 "\"number\": 34, "
                +                 "\"street\": \"Claude Debussylaan\", "
                +                 "\"city\": \"Amsterdam\""
                +             "}, "
                +             "\"phone\": 123, "
                +             "\"scores\": [1.2, 3.4, 5.6]"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.address.missing", objId, obj, updateObj, updateField(),
                new ExpectedUpdate(patch, full, updated, null));
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
        String patch = "{"
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
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"phone\": {\"$numberLong\": \"123\"}, "
                +    "\"addresses\": [{"
                +          "\"street\": \"Claude Debussylaan\", "
                +          "\"city\": \"Amsterdam\""
                +    "},"
                +    "{"
                +          "\"street\": \"Fragkokklisias\","
                +          "\"city\": \"Athens\""
                +    "}], "
                +    "\"active\": true, "
                +    "\"scores\": [1.2, 3.4, 5.6]"
                + "}";
        final String updated = "{"
                +             "\"active\": true, "
                +             "\"addresses\": [{"
                +                  "\"street\": \"Claude Debussylaan\", "
                +                  "\"city\": \"Amsterdam\""
                +             "}, "
                +             "{"
                +                  "\"street\": \"Fragkokklisias\", "
                +                  "\"city\": \"Athens\""
                +             "}], "
                +             "\"phone\": 123, "
                +             "\"scores\": [1.2, 3.4, 5.6]"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name,*.c1.addresses.number", objId, obj, updateObj, updateField(),
                new ExpectedUpdate(patch, full, updated, null));
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
        String patch = "{"
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
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"phone\": {\"$numberLong\": \"123\"}, "
                +    "\"addresses\": [[{"
                +          "\"number\": {\"$numberLong\": \"34\"},"
                +          "\"street\": \"Claude Debussylaan\", "
                +          "\"city\": \"Amsterdam\""
                +    "}],"
                +    "[{"
                +          "\"number\": {\"$numberLong\": \"7\"},"
                +          "\"street\": \"Fragkokklisias\","
                +          "\"city\": \"Athens\""
                +    "}]], "
                +    "\"active\": true, "
                +    "\"scores\": [1.2, 3.4, 5.6]"
                + "}";
        final String updated = "{"
                +             "\"active\": true, "
                +             "\"addresses\": [[{"
                +                  "\"number\": 34, "
                +                  "\"street\": \"Claude Debussylaan\", "
                +                  "\"city\": \"Amsterdam\""
                +             "}], "
                +             "[{"
                +                  "\"number\": 7, "
                +                  "\"street\": \"Fragkokklisias\", "
                +                  "\"city\": \"Athens\""
                +             "}]], "
                +             "\"phone\": 123, "
                +             "\"scores\": [1.2, 3.4, 5.6]"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name,*.c1.addresses.number", objId, obj, updateObj, updateField(),
                new ExpectedUpdate(patch, full, updated, null));
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
        String patch = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"phone\": {\"$numberLong\": \"123\"}"
                +     "}"
                + "}";
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"phone\": {\"$numberLong\": \"123\"}"
                + "}";
        final String updated = "{"
                +             "\"phone\": 123"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name", objId, obj, updateObj, updateField(),
                new ExpectedUpdate(patch, full, updated, null));
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
        String patch = "{"
                +     "\"$v\": 1,"
                +     "\"$unset\": {"
                +          "\"phone\": true"
                +     "}"
                + "}";
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"active\": true, "
                +    "\"scores\": [1.2, 3.4, 5.6]"
                + "}";
        final String updated = "{"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name", objId, obj, updateObj, false, updateField(),
                new ExpectedUpdate(patch, full, updated,
                        Arrays.asList("phone")));
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
        String patch = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"address\": {"
                +             "\"street\": \"Claude Debussylaan\","
                +             "\"city\": \"Amsterdam\""
                +         "},"
                +         "\"phone\": {\"$numberLong\": \"123\"}"
                +     "}"
                + "}";
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"phone\": {\"$numberLong\": \"123\"}, "
                +          "\"address\": {"
                +              "\"street\": \"Claude Debussylaan\", "
                +              "\"city\": \"Amsterdam\""
                +          "}"
                + "}";
        final String updated = "{"
                +             "\"address\": {"
                +                 "\"street\": \"Claude Debussylaan\", "
                +                 "\"city\": \"Amsterdam\""
                +             "}, "
                +             "\"phone\": 123"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name,*.c1.address.number", objId, obj, updateObj, updateField(),
                new ExpectedUpdate(patch, full, updated, null));
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
        String patch = "{"
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
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"phone\": {\"$numberLong\": \"123\"}, "
                +    "\"addresses\": [{"
                +          "\"street\": \"Claude Debussylaan\", "
                +          "\"city\": \"Amsterdam\""
                +    "},"
                +    "{"
                +          "\"street\": \"Fragkokklisias\","
                +          "\"city\": \"Athens\""
                +    "}]"
                + "}";
        final String updated = "{"
                +             "\"addresses\": [{"
                +                  "\"street\": \"Claude Debussylaan\", "
                +                  "\"city\": \"Amsterdam\""
                +             "}, "
                +             "{"
                +                  "\"street\": \"Fragkokklisias\", "
                +                  "\"city\": \"Athens\""
                +             "}], "
                +             "\"phone\": 123"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name,*.c1.addresses.number", objId, obj, updateObj, updateField(),
                new ExpectedUpdate(patch, full, updated, null));
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
        String patch = "{"
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
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"phone\": {\"$numberLong\": \"123\"}, "
                +    "\"addresses\": [[{"
                +          "\"number\": {\"$numberLong\": \"34\"},"
                +          "\"street\": \"Claude Debussylaan\", "
                +          "\"city\": \"Amsterdam\""
                +    "}],"
                +    "[{"
                +          "\"number\": {\"$numberLong\": \"7\"},"
                +          "\"street\": \"Fragkokklisias\","
                +          "\"city\": \"Athens\""
                +    "}]]"
                + "}";
        final String updated = "{"
                +             "\"addresses\": [[{"
                +                  "\"number\": 34, "
                +                  "\"street\": \"Claude Debussylaan\", "
                +                  "\"city\": \"Amsterdam\""
                +             "}], "
                +             "[{"
                +                  "\"number\": 7, "
                +                  "\"street\": \"Fragkokklisias\", "
                +                  "\"city\": \"Athens\""
                +             "}]], "
                +             "\"phone\": 123"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name,*.c1.addresses.number", objId, obj, updateObj, updateField(),
                new ExpectedUpdate(patch, full, updated, null));
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
        String patch = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"address.city\": \"Amsterdam\","
                +         "\"address.street\": \"Claude Debussylaan\""
                +     "}"
                + "}";
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"phone\": {\"$numberLong\": \"456\"}, "
                +          "\"address\": {"
                +              "\"street\": \"Claude Debussylaan\", "
                +              "\"city\": \"Amsterdam\""
                +          "}"
                + "}";
        final String updated = "{"
                +             "\"address.city\": \"Amsterdam\", "
                +             "\"address.street\": \"Claude Debussylaan\""
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name,*.c1.address.number", objId, obj, updateObj, updateField(),
                new ExpectedUpdate(patch, full, updated, null));
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
        String patch = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"addresses.0.city\": \"Amsterdam\","
                +         "\"addresses.0.street\": \"Claude Debussylaan\","
                +         "\"name\": \"Sally\""
                +     "}"
                + "}";
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"name\": \"Sally\", "
                +    "\"addresses\": [{"
                +          "\"street\": \"Claude Debussylaan\", "
                +          "\"city\": \"Amsterdam\""
                +    "}]"
                + "}";
        final String updated = "{"
                +             "\"addresses.0.city\": \"Amsterdam\", "
                +             "\"addresses.0.street\": \"Claude Debussylaan\", "
                +             "\"name\": \"Sally\""
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.addresses.number", objId, obj, updateObj, updateField(),
                new ExpectedUpdate(patch, full, updated, null));
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
        String patch = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"addresses.0.0.city\": \"Amsterdam\","
                +         "\"addresses.0.0.number\": {\"$numberLong\": \"34\"},"
                +         "\"addresses.0.0.street\": \"Claude Debussylaan\","
                +         "\"name\": \"Sally\""
                +     "}"
                + "}";
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"name\": \"Sally\", "
                +    "\"addresses\": [[{"
                +          "\"number\": {\"$numberLong\": \"34\"},"
                +          "\"street\": \"Claude Debussylaan\", "
                +          "\"city\": \"Amsterdam\""
                +    "}],"
                +    "[{"
                +          "\"number\": {\"$numberLong\": \"8\"},"
                +          "\"street\": \"Fragkokklisiass\","
                +          "\"city\": \"Athense\""
                +    "}]]"
                + "}";
        final String updated = "{"
                +             "\"addresses.0.0.city\": \"Amsterdam\", "
                +             "\"addresses.0.0.number\": 34, "
                +             "\"addresses.0.0.street\": \"Claude Debussylaan\", "
                +             "\"name\": \"Sally\""
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.addresses.number", objId, obj, updateObj, updateField(),
                new ExpectedUpdate(patch, full, updated, null));
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
        String patch = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"addresses.0.second.0.city\": \"Amsterdam\","
                +         "\"addresses.0.second.0.street\": \"Claude Debussylaan\","
                +         "\"name\": \"Sally\""
                +     "}"
                + "}";
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"name\": \"Sally\", "
                +    "\"addresses\": [{\"second\": [{"
                +          "\"street\": \"Claude Debussylaan\", "
                +          "\"city\": \"Amsterdam\""
                +    "}]}]"
                + "}";
        final String updated = "{"
                +             "\"addresses.0.second.0.city\": \"Amsterdam\", "
                +             "\"addresses.0.second.0.street\": \"Claude Debussylaan\", "
                +             "\"name\": \"Sally\""
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.addresses.second.number", objId, obj, updateObj, updateField(),
                new ExpectedUpdate(patch, full, updated, null));
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
        String patch = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"name\": \"Sally\""
                +     "}"
                + "}";
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"name\": \"Sally\""
                + "}";
        final String updated = "{"
                +             "\"name\": \"Sally\""
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.addresses", objId, obj, updateObj, updateField(),
                new ExpectedUpdate(patch, full, updated, null));
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
        String patch = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"name\": \"Sally\""
                +     "}"
                + "}";
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"name\": \"Sally\""
                + "}";
        final String updated = "{"
                +             "\"name\": \"Sally\""
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.addresses", objId, obj, updateObj, updateField(),
                new ExpectedUpdate(patch, full, updated, null));
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
        String patch = "{"
                +     "\"$v\": 1,"
                +     "\"$unset\": {"
                +         "\"address.city\": true,"
                +         "\"address.street\": true"
                +     "}"
                + "}";
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"phone\": {\"$numberLong\": \"456\"}, "
                +    "\"address\": {"
                +    "}, "
                +    "\"active\": false, "
                +    "\"scores\": [1.2, 3.4, 5.6, 7.8]"
                + "}";
        final String updated = "{"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.name,*.c1.address.number", objId, obj, updateObj, false, updateField(),
                new ExpectedUpdate(patch, full, updated, Arrays.asList("address.city", "address.street")));
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
        String patch = "{"
                +     "\"$v\": 1,"
                +     "\"$unset\": {"
                +         "\"addresses.0.city\": true,"
                +         "\"addresses.0.street\": true,"
                +         "\"name\": true"
                +     "}"
                + "}";
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"phone\": {\"$numberLong\": \"123\"}, "
                +    "\"addresses\": [{"
                +    "},"
                +    "{"
                +          "\"street\": \"Fragkokklisias\","
                +          "\"city\": \"Athens\""
                +    "}], "
                +    "\"active\": true, "
                +    "\"scores\": [1.2, 3.4, 5.6]"
                + "}";
        final String updated = "{"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.addresses.number", objId, obj, updateObj, false, updateField(), new ExpectedUpdate(
                patch, full, updated, Arrays.asList("addresses.0.city", "addresses.0.street", "name")));
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
        String patch = "{"
                +     "\"$v\": 1,"
                +     "\"$unset\": {"
                +         "\"addresses.0.0.city\": true,"
                +         "\"addresses.0.0.number\": true,"
                +         "\"addresses.0.0.street\": true,"
                +         "\"name\": true"
                +     "}"
                + "}";
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"phone\": {\"$numberLong\": \"123\"}, "
                +    "\"addresses\": [[{"
                +    "}]], "
                +    "\"active\": true, "
                +    "\"scores\": [1.2, 3.4, 5.6]"
                + "}";
        final String updated = "{"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.addresses.number", objId, obj, updateObj, false, updateField(),
                new ExpectedUpdate(patch, full, updated,
                        Arrays.asList("addresses.0.0.city", "addresses.0.0.number", "addresses.0.0.street", "name")));
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
        String patch = "{"
                +     "\"$v\": 1,"
                +     "\"$unset\": {"
                +         "\"addresses.0.second.0.city\": true,"
                +         "\"addresses.0.second.0.street\": true,"
                +         "\"name\": true"
                +     "}"
                + "}";
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}, "
                +    "\"addresses\": [{\"second\": [{"
                +    "}]}]"
                + "}";
        final String updated = "{"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.addresses.second.number", objId, obj, updateObj, false, updateField(),
                new ExpectedUpdate(patch, full, updated,
                        Arrays.asList("addresses.0.second.0.city", "addresses.0.second.0.street", "name")));
    }

    @Test
    public void shouldExcludeFieldsForUnsetNestedFieldUpdateEventWithArrayOfEmbeddedDocuments() throws InterruptedException {
        // TODO Fix for oplog
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
        String patch = "{"
                +     "\"$v\": 1,"
                +     "\"$unset\": {"
                +         "\"name\": true"
                +     "}"
                + "}";
        String full = "{"
                +    "\"_id\": {\"$oid\": \"<OID>\"}"
                + "}";
        final String updated = "{"
                + "}";
        // @formatter:on

        assertUpdateRecord("*.c1.addresses", objId, obj, updateObj, false, updateField(),
                new ExpectedUpdate(patch, full, updated,
                        Arrays.asList("name")));
    }

    @Test
    public void shouldExcludeFieldsForDeleteEvent() throws InterruptedException {
        config = getConfiguration("*.c1.name,*.c1.active");
        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbA");

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

        assertThat(json).isNull();
    }

    @Test
    public void shouldExcludeFieldsForDeleteTombstoneEvent() throws InterruptedException {
        config = getConfiguration("*.c1.name,*.c1.active");
        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbA");

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

    private Configuration getConfiguration(String excludeList) {
        return TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.FIELD_EXCLUDE_LIST, excludeList)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbA.c1")
                .with(CommonConnectorConfig.TOPIC_PREFIX, SERVER_NAME)
                .build();
    }

    private Struct getValue(SourceRecord record) {
        return (Struct) record.value();
    }

    private void storeDocuments(String dbName, String collectionName, Document... documents) {
        try (var client = connect()) {
            Testing.debug("Storing in '" + dbName + "." + collectionName + "' document");
            MongoDatabase db = client.getDatabase(dbName);
            MongoCollection<Document> coll = db.getCollection(collectionName);
            coll.drop();

            for (Document document : documents) {
                InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
                assertThat(document).isNotNull();
                assertThat(document.size()).isGreaterThan(0);
                coll.insertOne(document, insertOptions);
            }
        }
    }

    private void updateDocuments(String dbName, String collectionName, ObjectId objId, Document document, boolean doSet) {
        try (var client = connect()) {
            MongoDatabase db = client.getDatabase(dbName);
            MongoCollection<Document> coll = db.getCollection(collectionName);
            Document filter = Document.parse("{\"_id\": {\"$oid\": \"" + objId + "\"}}");
            coll.updateOne(filter, new Document().append(doSet ? "$set" : "$unset", document));
        }
    }

    private void deleteDocuments(String dbName, String collectionName, ObjectId objId) {
        try (var client = connect()) {
            MongoDatabase db = client.getDatabase(dbName);
            MongoCollection<Document> coll = db.getCollection(collectionName);
            Document filter = Document.parse("{\"_id\": {\"$oid\": \"" + objId + "\"}}");
            coll.deleteOne(filter);
        }
    }

    private void assertReadRecord(String blackList, Document snapshotRecord, String field, String expected) throws InterruptedException {
        config = getConfiguration(blackList);
        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbA");
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

        TestHelper.cleanDatabase(mongo, "dbA");

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
                                    String field, ExpectedUpdate expected)
            throws InterruptedException {
        assertUpdateRecord(blackList, objectId, snapshotRecord, updateRecord, true, field, expected);
    }

    private void assertUpdateRecord(String blackList, ObjectId objectId, Document snapshotRecord, Document updateRecord,
                                    boolean doSet, String field, ExpectedUpdate expected)
            throws InterruptedException {
        config = getConfiguration(blackList);
        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbA");

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

        TestHelper.assertChangeStreamUpdateAsDocs(objectId, value, expected.full, expected.removedFields,
                expected.updatedFields);
    }

    private String updateField() {
        return AFTER;
    }
}
