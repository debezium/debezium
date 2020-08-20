/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.connector.mongodb.MongoDbSchema.COMPACT_JSON_SETTINGS;
import static io.debezium.data.Envelope.FieldName.AFTER;
import static org.fest.assertions.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.junit.logging.LogInterceptor;

/**
 * @author Chris Cranford
 */
public class FieldRenamesIT extends AbstractMongoConnectorIT {

    private static final String DATABASE_NAME = "dbA";
    private static final String COLLECTION_NAME = "c1";
    private static final String SERVER_NAME = "serverX";
    private static final String PATCH = "patch";
    private static final String ID = "_id";

    @Test
    public void shouldNotRenameMissingFieldsForReadEvent() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append(ID, objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        SourceRecord record = getReadRecord("*.c1.missing:new_missing", obj);

        Struct value = (Struct) record.value();
        assertThat(value.get(AFTER)).isEqualTo(obj.toJson(COMPACT_JSON_SETTINGS));
    }

    @Test
    public void shouldNotRenameNestedMissingFieldsForReadEvent() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append(ID, objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("address", new Document()
                        .append("number", 34L)
                        .append("street", "Claude Debussylaan")
                        .append("city", "Amsterdam"))
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        SourceRecord record = getReadRecord("*.c1.address.missing:new_missing", obj);

        Struct value = (Struct) record.value();
        assertThat(value.get(AFTER)).isEqualTo(obj.toJson(COMPACT_JSON_SETTINGS));
    }

    @Test
    public void shouldNotRenameMissingFieldsForInsertEvent() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append(ID, objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        SourceRecord record = getInsertRecord("*.c1.missing:new_missing", obj);

        Struct value = (Struct) record.value();
        assertThat(value.get(AFTER)).isEqualTo(obj.toJson(COMPACT_JSON_SETTINGS));
    }

    @Test
    public void shouldNotRenameNestedMissingFieldsForInsertEvent() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append(ID, objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("address", new Document()
                        .append("number", 34L)
                        .append("street", "Claude Debussylaan")
                        .append("city", "Amsterdam"))
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        SourceRecord record = getInsertRecord("*.c1.address.missing:new_missing", obj);

        Struct value = (Struct) record.value();
        assertThat(value.get(AFTER)).isEqualTo(obj.toJson(COMPACT_JSON_SETTINGS));
    }

    @Test
    public void shouldNotRenameNestedMissingFieldsForUpdateEventWithEmbeddedDocument() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append(ID, objId)
                .append("name", "Sally May")
                .append("phone", 456L)
                .append("address", new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame"))
                .append("active", false)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6, 7.8));

        Document updateObj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("phone", 123L)
                        .append("address", new Document()
                                .append("number", 34L)
                                .append("street", "Claude Debussylaan")
                                .append("city", "Amsterdam"))
                        .append("active", true)
                        .append("scores", Arrays.asList(1.2, 3.4, 5.6)));

        SourceRecord record = getUpdateRecord("*.c1.address.missing:new_missing", obj, updateObj);

        Struct value = (Struct) record.value();
        assertThat(getDocumentFromPatch(value)).isEqualTo(updateObj);
    }

    @Test
    public void shouldNotRenameFieldsForEventOfOtherCollection() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        SourceRecord record = getReadRecord("*.c2.name:new_name,*.c2.active:new_active", obj);

        Struct value = (Struct) record.value();
        assertThat(value.get(AFTER)).isEqualTo(obj.toJson(COMPACT_JSON_SETTINGS));
    }

    @Test
    public void shouldRenameFieldsForReadEvent() throws Exception {
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
                +     "\"scores\": [1.2,3.4,5.6],"
                +     "\"new_name\": \"Sally\","
                +     "\"new_active\": true"
                + "}";
        // @formatter:on

        SourceRecord record = getReadRecord("*.c1.name:new_name,*.c1.active:new_active", obj);

        Struct value = (Struct) record.value();
        assertThat(value.get(AFTER)).isEqualTo(expected);
    }

    @Test
    public void shouldRenameNestedFieldsForReadEvent() throws Exception {
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
                +         "\"city\": \"Amsterdam\","
                +         "\"new_number\": {\"$numberLong\": \"34\"}"
                +     "},"
                +     "\"scores\": [1.2,3.4,5.6],"
                +     "\"new_name\": \"Sally\","
                +     "\"new_active\": true"
                + "}";
        // @formatter:on

        SourceRecord record = getReadRecord("*.c1.name:new_name,*.c1.active:new_active,*.c1.address.number:new_number", obj);

        Struct value = (Struct) record.value();
        assertThat(value.get(AFTER)).isEqualTo(expected);
    }

    @Test
    public void shouldNotRenameNestedFieldsToExistingNamesForReadEvent() throws Exception {
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

        assertShouldNotRenameDuringRead("*.c1.address.street:city", obj, "city");
    }

    @Test
    public void shouldRenameFieldsForInsertEvent() throws Exception {
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
                +     "\"scores\": [1.2,3.4,5.6],"
                +     "\"new_name\": \"Sally\","
                +     "\"new_active\": true"
                + "}";
        // @formatter:on

        SourceRecord record = getInsertRecord("*.c1.name:new_name,*.c1.active:new_active", obj);

        Struct value = (Struct) record.value();
        assertThat(value.get(AFTER)).isEqualTo(expected);
    }

    @Test
    public void shouldRenameNestedFieldsForInsertEvent() throws Exception {
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
                +         "\"city\": \"Amsterdam\","
                +         "\"new_number\": {\"$numberLong\": \"34\"}"
                +     "},"
                +     "\"scores\": [1.2,3.4,5.6],"
                +     "\"new_name\": \"Sally\","
                +     "\"new_active\": true"
                + "}";
        // @formatter:on

        SourceRecord record = getInsertRecord("*.c1.name:new_name,*.c1.active:new_active,*.c1.address.number:new_number", obj);

        Struct value = (Struct) record.value();
        assertThat(value.get(AFTER)).isEqualTo(expected);
    }

    @Test
    public void shouldNotRenameNestedFieldsToExistingNamesForInsertEvent() throws Exception {
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

        assertShouldNotRenameDuringInsert("*.c1.address.street:city", obj, "city");
    }

    @Test
    public void shouldRenameFieldsForUpdateEvent() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("phone", 456L)
                .append("active", false)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6, 7.8));

        Document updateObj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("phone", 123L)
                        .append("active", true)
                        .append("scores", Arrays.asList(1.2, 3.4, 5.6)));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +          "\"phone\": {\"$numberLong\": \"123\"},"
                +          "\"scores\": [1.2,3.4,5.6],"
                +          "\"new_name\": \"Sally\","
                +          "\"new_active\": true"
                +     "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.name:new_name,*.c1.active:new_active", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldNotRenameMissingFieldsForUpdateEvent() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("phone", 456L)
                .append("active", false)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6, 7.8));

        Document updateObj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("phone", 123L)
                        .append("active", true)
                        .append("scores", Arrays.asList(1.2, 3.4, 5.6)));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +          "\"active\": true,"
                +          "\"name\": \"Sally\","
                +          "\"phone\": {\"$numberLong\": \"123\"},"
                +          "\"scores\": [1.2,3.4,5.6]"
                +     "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.missing:new_missing", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldRenameNestedFieldsForUpdateEventWithEmbeddedDocument() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("phone", 456L)
                .append("address", new Document()
                        .append("number", 56L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame"))
                .append("active", false)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6, 7.8));

        Document updateObj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("phone", 123L)
                        .append("address", new Document()
                                .append("number", 34L)
                                .append("street", "Claude Debussylaan")
                                .append("city", "Amsterdam"))
                        .append("active", true)
                        .append("scores", Arrays.asList(1.2, 3.4, 5.6)));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +          "\"address\": {"
                +              "\"street\": \"Claude Debussylaan\","
                +              "\"city\": \"Amsterdam\","
                +              "\"new_number\": {\"$numberLong\": \"34\"}"
                +          "},"
                +          "\"phone\": {\"$numberLong\": \"123\"},"
                +          "\"scores\": [1.2,3.4,5.6],"
                +          "\"new_name\": \"Sally\","
                +          "\"new_active\": true"
                +     "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.name:new_name,*.c1.active:new_active,*.c1.address.number:new_number", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldNotRenameNestedFieldsToExistingNamesForUpdateEventWithEmbeddedDocument() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("phone", 456L)
                .append("address", new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame"))
                .append("active", false)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6, 7.8));

        Document updateObj = new Document()
                .append("name", "Sally")
                .append("phone", 123L)
                .append("address", new Document()
                        .append("number", 34L)
                        .append("street", "Claude Debussylaan")
                        .append("city", "Amsterdam"))
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        assertShouldNotRenameDuringUpdate("*.c1.address.street:city", obj, updateObj, false, "city");
    }

    @Test
    public void shouldRenameNestedFieldsForUpdateEventWithArrayOfEmbeddedDocuments() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("phone", 456L)
                .append("addresses", Arrays.asList(
                        new Document()
                                .append("number", 56L)
                                .append("street", "Claude Debussylaann")
                                .append("city", "Amsterdame"),
                        new Document()
                                .append("number", 8L)
                                .append("street", "Fragkokklisiass")
                                .append("city", "Athense")))
                .append("active", false)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6, 7.8));

        Document updateObj = new Document()
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
                                        .append("city", "Athens")))
                        .append("active", true)
                        .append("scores", Arrays.asList(1.2, 3.4, 5.6)));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +          "\"active\": true,"
                +          "\"addresses\": ["
                +              "{"
                +                  "\"street\": \"Claude Debussylaan\","
                +                  "\"city\": \"Amsterdam\","
                +                  "\"new_number\": {\"$numberLong\": \"34\"}"
                +              "},"
                +              "{"
                +                  "\"street\": \"Fragkokklisias\","
                +                  "\"city\": \"Athens\","
                +                  "\"new_number\": {\"$numberLong\": \"7\"}"
                +              "}"
                +          "],"
                +          "\"phone\": {\"$numberLong\": \"123\"},"
                +          "\"scores\": [1.2,3.4,5.6],"
                +          "\"new_name\": \"Sally\""
                +     "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.name:new_name,*.c1.addresses.number:new_number", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldNotRenameNestedFieldsForUpdateEventWithArrayOfArrays() throws Exception {
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
                                .append("city", "Athense"))))
                .append("active", false)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6, 7.8));

        Document updateObj = new Document()
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
                                        .append("city", "Athens"))))
                        .append("active", true)
                        .append("scores", Arrays.asList(1.2, 3.4, 5.6)));

        // then
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
                +          "\"scores\": [1.2,3.4,5.6],"
                +          "\"new_name\": \"Sally\""
                +     "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.name:new_name,*.c1.addresses.number:new_number", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldRenameFieldsForSetTopLevelFieldUpdateEvent() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("phone", 456L);

        Document updateObj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("phone", 123L));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"phone\": {\"$numberLong\": \"123\"},"
                +         "\"new_name\": \"Sally\""
                +     "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.name:new_name", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldNotRenameFieldsToExistingNamesForSetTopLevelFieldUpdateEvent() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("phone", 456L);

        Document updateObj = new Document()
                .append("name", "Sally")
                .append("phone", 123L);

        assertShouldNotRenameDuringUpdate("*.c1.name:phone", obj, updateObj, false, "phone");
    }

    @Test
    public void shouldRenameFieldsForUnsetTopLevelFieldUpdateEvent() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("phone", 456L);

        Document updateObj = new Document()
                .append("$unset", new Document()
                        .append("name", "")
                        .append("phone", ""));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$unset\": {"
                +         "\"phone\": true,"
                +         "\"new_name\": true"
                +     "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.name:new_name", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldNotRenameFieldsToExistingNamesForUnsetTopLevelFieldUpdateEvent() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("phone", 456L);

        Document updateObj = new Document()
                .append("name", "")
                .append("phone", "");

        assertShouldNotRenameDuringUpdate("*.c1.name:phone", obj, updateObj, false, "phone");
    }

    @Test
    public void shouldRenameNestedFieldsForSetTopLevelFieldUpdateEventWithEmbeddedDocument() throws Exception {
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
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("phone", 123L)
                        .append("address", new Document()
                                .append("number", 34L)
                                .append("street", "Claude Debussylaan")
                                .append("city", "Amsterdam")));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"address\": {"
                +             "\"street\": \"Claude Debussylaan\","
                +             "\"city\": \"Amsterdam\","
                +             "\"new_number\": {\"$numberLong\": \"34\"}"
                +         "},"
                +         "\"phone\": {\"$numberLong\": \"123\"},"
                +         "\"new_name\": \"Sally\""
                +     "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.name:new_name,*.c1.address.number:new_number", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldRenameNestedFieldsForSetTopLevelFieldUpdateEventWithArrayOfEmbeddedDocuments() throws Exception {
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

        String expected = "{"
                + "\"$v\": 1,"
                + "\"$set\": {"
                + "\"addresses\": ["
                + "{"
                + "\"street\": \"Claude Debussylaan\","
                + "\"city\": \"Amsterdam\","
                + "\"new_number\": {\"$numberLong\": \"34\"}"
                + "},"
                + "{"
                + "\"street\": \"Fragkokklisias\","
                + "\"city\": \"Athens\","
                + "\"new_number\": {\"$numberLong\": \"7\"}"
                + "}"
                + "],"
                + "\"phone\": {\"$numberLong\": \"123\"},"
                + "\"new_name\": \"Sally\""
                + "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.name:new_name,*.c1.addresses.number:new_number", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldNotRenameNestedFieldsForSetTopLevelFieldUpdateEventWithArrayOfArrays() throws Exception {
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
                +         "\"phone\": {\"$numberLong\": \"123\"},"
                +         "\"new_name\": \"Sally\""
                +     "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.name:new_name,*.c1.addresses.number:new_number", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldRenameNestedFieldsForSetNestedFieldUpdateEventWithEmbeddedDocument() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("address", new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame"));

        Document updateObj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("address.number", 34L)
                        .append("address.street", "Claude Debussylaan")
                        .append("address.city", "Amsterdam"));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"address.city\": \"Amsterdam\","
                +         "\"address.street\": \"Claude Debussylaan\","
                +         "\"new_name\": \"Sally\","
                +         "\"address.new_number\": {\"$numberLong\": \"34\"}"
                +     "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.name:new_name,*.c1.address.number:new_number", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldRenameNestedFieldsForSetNestedFieldUpdateEventWithArrayOfEmbeddedDocuments() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("addresses", Arrays.asList(new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame")));

        Document updateObj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("addresses.0.number", 34L)
                        .append("addresses.0.street", "Claude Debussylaan")
                        .append("addresses.0.city", "Amsterdam"));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"addresses.0.city\": \"Amsterdam\","
                +         "\"addresses.0.street\": \"Claude Debussylaan\","
                +         "\"name\": \"Sally\","
                +         "\"addresses.0.new_number\": {\"$numberLong\": \"34\"}"
                +     "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.addresses.number:new_number", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldNotRenameNestedFieldsToExistingNamesForSetNestedFieldUpdateEventWithArrayOfEmbeddedDocuments() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("addresses", Arrays.asList(new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame")));

        Document updateObj = new Document()
                .append("name", "Sally")
                .append("addresses.0.number", 34L)
                .append("addresses.0.street", "Claude Debussylaan")
                .append("addresses.0.city", "Amsterdam");

        assertShouldNotRenameDuringUpdate("*.c1.addresses.street:city", obj, updateObj, false, "addresses.0.city");
    }

    @Test
    public void shouldNotRenameNestedFieldsForSetNestedFieldUpdateEventWithArrayOfArrays() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("addresses", Arrays.asList(Arrays.asList(new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame"))));

        Document updateObj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("addresses.0.0.number", 34L)
                        .append("addresses.0.0.street", "Claude Debussylaan")
                        .append("addresses.0.0.city", "Amsterdam"));

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

        SourceRecord record = getUpdateRecord("*.c1.addresses.number:new_number", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldRenameNestedFieldsForSetNestedFieldUpdateEventWithSeveralArrays() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("addresses", Arrays.asList(Collections.singletonMap("second", new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame"))));

        Document updateObj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("addresses.0.second.0.number", 34L)
                        .append("addresses.0.second.0.street", "Claude Debussylaan")
                        .append("addresses.0.second.0.city", "Amsterdam"));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"addresses.0.second.0.city\": \"Amsterdam\","
                +         "\"addresses.0.second.0.street\": \"Claude Debussylaan\","
                +         "\"name\": \"Sally\","
                +         "\"addresses.0.second.0.new_number\": {\"$numberLong\": \"34\"}"
                +     "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.addresses.second.number:new_number", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldRenameFieldsForSetNestedFieldUpdateEventWithArrayOfEmbeddedDocuments() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("addresses", Arrays.asList(new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame")));

        Document updateObj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("addresses.0.number", 34L)
                        .append("addresses.0.street", "Claude Debussylaan")
                        .append("addresses.0.city", "Amsterdam"));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"name\": \"Sally\","
                +         "\"new_addresses.0.city\": \"Amsterdam\","
                +         "\"new_addresses.0.number\": {\"$numberLong\": \"34\"},"
                +         "\"new_addresses.0.street\": \"Claude Debussylaan\""
                +     "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.addresses:new_addresses", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldRenameFieldsForSetToArrayFieldUpdateEventWithArrayOfEmbeddedDocuments() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("addresses", Arrays.asList(new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame")));

        Document updateObj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally")
                        .append("addresses.0", new Document()
                                .append("number", 34L)
                                .append("street", "Claude Debussylaan")
                                .append("city", "Amsterdam")));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$set\": {"
                +         "\"name\": \"Sally\","
                +         "\"new_addresses.0\": {"
                +             "\"number\": {\"$numberLong\": \"34\"},"
                +             "\"street\": \"Claude Debussylaan\","
                +             "\"city\": \"Amsterdam\""
                +         "}"
                +     "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.addresses:new_addresses", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldRenameNestedFieldsForUnsetNestedFieldUpdateEventWithEmbeddedDocument() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("address", new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame"));

        Document updateObj = new Document()
                .append("$unset", new Document()
                        .append("name", "")
                        .append("address.number", "")
                        .append("address.street", "")
                        .append("address.city", ""));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$unset\": {"
                +         "\"address.city\": true,"
                +         "\"address.street\": true,"
                +         "\"new_name\": true,"
                +         "\"address.new_number\": true"
                +     "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.name:new_name,*.c1.address.number:new_number", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldRenameNestedFieldsForUnsetNestedFieldUpdateEventWithArrayOfEmbeddedDocuments() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("addresses", Arrays.asList(new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame")));

        Document updateObj = new Document()
                .append("$unset", new Document()
                        .append("name", "")
                        .append("addresses.0.number", "")
                        .append("addresses.0.street", "")
                        .append("addresses.0.city", ""));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$unset\": {"
                +         "\"addresses.0.city\": true,"
                +         "\"addresses.0.street\": true,"
                +         "\"name\": true,"
                +         "\"addresses.0.new_number\": true"
                +     "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.addresses.number:new_number", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldNotRenameNestedFieldsToExistingNamesForUnsetNestedFieldUpdateEventWithArrayOfEmbeddedDocuments() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("addresses", Arrays.asList(new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame")));

        Document updateObj = new Document()
                .append("name", "")
                .append("addresses.0.number", "")
                .append("addresses.0.street", "")
                .append("addresses.0.city", "");

        assertShouldNotRenameDuringUpdate("*.c1.addresses.street:city", obj, updateObj, true, "addresses.0.city");
    }

    @Test
    public void shouldNotRenameNestedFieldsForUnsetNestedFieldUpdateEventWithArrayOfArrays() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("addresses", Arrays.asList(Arrays.asList(new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame"))));

        Document updateObj = new Document()
                .append("$unset", new Document()
                        .append("name", "")
                        .append("addresses.0.0.number", "")
                        .append("addresses.0.0.street", "")
                        .append("addresses.0.0.city", ""));

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

        SourceRecord record = getUpdateRecord("*.c1.addresses.number:new_number", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldRenameNestedFieldsForUnsetNestedFieldUpdateEventWithSeveralArrays() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("addresses", Arrays.asList(Collections.singletonMap("second", Arrays.asList(new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame")))));

        Document updateObj = new Document()
                .append("$unset", new Document()
                        .append("name", "")
                        .append("addresses.0.second.0.number", "")
                        .append("addresses.0.second.0.street", "")
                        .append("addresses.0.second.0.city", ""));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$unset\": {"
                +         "\"addresses.0.second.0.city\": true,"
                +         "\"addresses.0.second.0.street\": true,"
                +         "\"name\": true,"
                +         "\"addresses.0.second.0.new_number\": true"
                +     "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.addresses.second.number:new_number", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldRenameFieldsForUnsetNestedFieldUpdateEventWithArrayOfEmbeddedDocuments() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally May")
                .append("addresses", Arrays.asList(new Document()
                        .append("number", 45L)
                        .append("street", "Claude Debussylaann")
                        .append("city", "Amsterdame")));

        Document updateObj = new Document()
                .append("$unset", new Document()
                        .append("name", "")
                        .append("addresses.0.number", "")
                        .append("addresses.0.street", "")
                        .append("addresses.0.city", ""));

        // @formatter:off
        String expected = "{"
                +     "\"$v\": 1,"
                +     "\"$unset\": {"
                +         "\"name\": true,"
                +         "\"new_addresses.0.city\": true,"
                +         "\"new_addresses.0.number\": true,"
                +         "\"new_addresses.0.street\": true"
                +     "}"
                + "}";
        // @formatter:on

        SourceRecord record = getUpdateRecord("*.c1.addresses:new_addresses", obj, updateObj);

        Struct value = (Struct) record.value();
        final Document actualDoc = TestHelper.getDocumentWithoutLanguageVersion(value.getString(PATCH));
        final Document expectedDoc = TestHelper.getDocumentWithoutLanguageVersion(expected);
        assertThat(actualDoc).isEqualTo(expectedDoc);
    }

    @Test
    public void shouldRenameFieldsForDeleteEvent() throws Exception {
        config = getConfiguration("*.c1.name:new_name,*.c1.active:new_active");
        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(primary(), "dbA");

        ObjectId objId = new ObjectId();
        Document obj = new Document("_id", objId);
        dropAndInsertDocuments("dbA", "c1", obj);

        start(MongoDbConnector.class, config);

        SourceRecords snapshotRecords = consumeRecordsByTopic(1);
        assertThat(snapshotRecords.topics().size()).isEqualTo(1);
        assertThat(snapshotRecords.allRecordsInOrder().size()).isEqualTo(1);

        // Wait for streaming to start and perform an update
        waitForStreamingRunning("mongodb", SERVER_NAME);
        deleteDocuments("dbA", "c1", getFilterFromId(objId));

        // Get the delete records (1 delete and 1 tombstone)
        SourceRecords deleteRecords = consumeRecordsByTopic(2);
        assertThat(deleteRecords.topics().size()).isEqualTo(1);
        assertThat(deleteRecords.allRecordsInOrder().size()).isEqualTo(2);

        // Only validating delete record, non-tombstone
        SourceRecord record = deleteRecords.allRecordsInOrder().get(0);

        Struct value = (Struct) record.value();
        String json = value.getString(AFTER);
        if (json == null) {
            json = value.getString(PATCH);
        }
        assertThat(json).isNull();
    }

    @Test
    public void shouldRenameFieldsForDeleteTombstoneEvent() throws Exception {
        config = getConfiguration("*.c1.name:new_name,*.c1.active:new_active");
        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(primary(), "dbA");

        ObjectId objId = new ObjectId();
        Document obj = new Document("_id", objId);
        dropAndInsertDocuments("dbA", "c1", obj);

        start(MongoDbConnector.class, config);

        SourceRecords snapshotRecords = consumeRecordsByTopic(1);
        assertThat(snapshotRecords.topics().size()).isEqualTo(1);
        assertThat(snapshotRecords.allRecordsInOrder().size()).isEqualTo(1);

        // Wait for streaming to start and perform an update
        waitForStreamingRunning("mongodb", SERVER_NAME);
        deleteDocuments("dbA", "c1", getFilterFromId(objId));

        // Get the delete records (1 delete and 1 tombstone)
        SourceRecords deleteRecords = consumeRecordsByTopic(2);
        assertThat(deleteRecords.topics().size()).isEqualTo(1);
        assertThat(deleteRecords.allRecordsInOrder().size()).isEqualTo(2);

        // Only validating tombstone record, non-delete
        SourceRecord record = deleteRecords.allRecordsInOrder().get(1);

        Struct value = (Struct) record.value();
        assertThat(value).isNull();
    }

    private static Document getFilterFromId(ObjectId id) {
        return Document.parse("{\"" + ID + "\": {\"$oid\": \"" + id + "\"}}");
    }

    private static Document getDocumentFromPatch(Struct value) {
        assertThat(value).isNotNull();

        final String patch = value.getString(PATCH);
        assertThat(patch).isNotNull();

        // By parsing the patch string, we can remove the $v internal key added by the driver that specifies the
        // language version used to manipulate the document. The goal by removing this key is that the original
        // document used to update the database entry can be compared directly.
        Document parsed = Document.parse(patch);
        parsed.remove("$v");
        return parsed;
    }

    private static Configuration getConfiguration(String fieldRenames) {
        return getConfiguration(fieldRenames, DATABASE_NAME, COLLECTION_NAME);
    }

    private static Configuration getConfiguration(String fieldRenames, String database, String collection) {
        Configuration.Builder builder = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, database + "." + collection)
                .with(MongoDbConnectorConfig.LOGICAL_NAME, SERVER_NAME);

        if (fieldRenames != null && !"".equals(fieldRenames.trim())) {
            builder = builder.with(MongoDbConnectorConfig.FIELD_RENAMES, fieldRenames);
        }

        return builder.build();
    }

    private SourceRecord getReadRecord(String fieldRenames, Document document) throws Exception {
        return getReadRecord(DATABASE_NAME, COLLECTION_NAME, fieldRenames, document);
    }

    private SourceRecord getReadRecord(String database, String collection, String fieldRenames, Document document)
            throws Exception {
        config = getConfiguration(fieldRenames, database, collection);
        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(primary(), database);

        dropAndInsertDocuments(database, collection, document);

        logInterceptor = new LogInterceptor();
        start(MongoDbConnector.class, config);

        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        assertThat(sourceRecords.allRecordsInOrder().size()).isEqualTo(1);

        return sourceRecords.allRecordsInOrder().get(0);
    }

    private SourceRecord getInsertRecord(String fieldRenames, Document document) throws Exception {
        return getInsertRecord(DATABASE_NAME, COLLECTION_NAME, fieldRenames, document);
    }

    private SourceRecord getInsertRecord(String database, String collection, String fieldRenames, Document document) throws Exception {
        config = getConfiguration(fieldRenames, database, collection);
        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(primary(), database);

        insertDocuments(database, collection, document);

        logInterceptor = new LogInterceptor();
        start(MongoDbConnector.class, config);

        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        assertThat(sourceRecords.allRecordsInOrder().size()).isEqualTo(1);

        return sourceRecords.allRecordsInOrder().get(0);
    }

    private SourceRecord getUpdateRecord(String fieldRenames, Document snapshot, Document document) throws Exception {
        return getUpdateRecord(DATABASE_NAME, COLLECTION_NAME, fieldRenames, snapshot, document);
    }

    private SourceRecord getUpdateRecord(String database, String collection, String fieldRenames, Document snapshot,
                                         Document document)
            throws Exception {
        // Store the snapshot read and start the connector
        final SourceRecord readRecord = getReadRecord(database, collection, fieldRenames, snapshot);

        updateDocument(database, collection, getFilterFromId(snapshot.getObjectId(ID)), document);

        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        assertThat(sourceRecords.allRecordsInOrder().size()).isEqualTo(1);

        return sourceRecords.allRecordsInOrder().get(0);
    }

    private void assertDocumentContainsFieldError(String fieldName) {
        stopConnector(value -> {
            final String message = "IllegalArgumentException: Document already contains field : " + fieldName;
            assertThat(logInterceptor.containsStacktraceElement(message)).isTrue();
        });
    }

    private void assertShouldNotRenameDuringRead(String renamesList, Document snapshot, String fieldName) throws Exception {
        config = getConfiguration(renamesList);
        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(primary(), DATABASE_NAME);

        dropAndInsertDocuments(DATABASE_NAME, COLLECTION_NAME, snapshot);

        logInterceptor = new LogInterceptor();
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", SERVER_NAME);

        assertNoRecordsToConsume();
        assertDocumentContainsFieldError(fieldName);
    }

    private void assertShouldNotRenameDuringInsert(String renamesList, Document document, String fieldName) throws Exception {
        config = getConfiguration(renamesList);
        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(primary(), DATABASE_NAME);

        logInterceptor = new LogInterceptor();
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", SERVER_NAME);

        insertDocuments(DATABASE_NAME, COLLECTION_NAME, document);

        assertNoRecordsToConsume();
        assertDocumentContainsFieldError(fieldName);
    }

    private void assertShouldNotRenameDuringUpdate(String renamesList, Document snapshot, Document update, boolean unset, String fieldName)
            throws Exception {
        // do not apply renames during snapshot
        final SourceRecord snapshotRecord = getReadRecord(DATABASE_NAME, COLLECTION_NAME, null, snapshot);

        // Wait for streaming to start and stop the connector
        waitForStreamingRunning("mongodb", SERVER_NAME);
        stopConnector();

        // reconfigure with renames and restart connector
        config = getConfiguration(renamesList, DATABASE_NAME, COLLECTION_NAME);
        context = new MongoDbTaskContext(config);

        logInterceptor = new LogInterceptor();
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", SERVER_NAME);

        final Document document = new Document().append(unset ? "$unset" : "$set", update);
        updateDocument(DATABASE_NAME, COLLECTION_NAME, getFilterFromId(snapshot.getObjectId(ID)), document);

        assertNoRecordsToConsume();
        assertDocumentContainsFieldError(fieldName);
    }
}
