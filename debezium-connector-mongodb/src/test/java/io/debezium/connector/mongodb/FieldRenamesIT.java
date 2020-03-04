/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.data.Envelope.FieldName.AFTER;
import static org.fest.assertions.Assertions.assertThat;

import java.util.Arrays;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Test;

import io.debezium.config.Configuration;

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
        assertThat(value.get(AFTER)).isEqualTo(obj.toJson(WRITER_SETTINGS));
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
        assertThat(value.get(AFTER)).isEqualTo(obj.toJson(WRITER_SETTINGS));
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
        assertThat(value.get(AFTER)).isEqualTo(obj.toJson(WRITER_SETTINGS));
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
        assertThat(value.get(AFTER)).isEqualTo(obj.toJson(WRITER_SETTINGS));
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

    private static Configuration getConfiguration(String fieldRenames, String database, String collection) {
        return TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.FIELD_RENAMES, fieldRenames)
                .with(MongoDbConnectorConfig.COLLECTION_WHITELIST, database + "." + collection)
                .with(MongoDbConnectorConfig.LOGICAL_NAME, SERVER_NAME)
                .build();
    }

    private SourceRecord getReadRecord(String fieldRenames, Document document) throws Exception {
        return getReadRecord(DATABASE_NAME, COLLECTION_NAME, fieldRenames, document);
    }

    private SourceRecord getReadRecord(String database, String collection, String fieldRenames, Document document) throws Exception {
        config = getConfiguration(fieldRenames, database, collection);
        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(primary(), database);

        dropAndInsertDocuments(database, collection, document);

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

        start(MongoDbConnector.class, config);

        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        assertThat(sourceRecords.allRecordsInOrder().size()).isEqualTo(1);

        return sourceRecords.allRecordsInOrder().get(0);
    }

    private SourceRecord getUpdateRecord(String fieldRenames, Document snapshot, Document document) throws Exception {
        return getUpdateRecord(DATABASE_NAME, COLLECTION_NAME, fieldRenames, snapshot, document);
    }

    private SourceRecord getUpdateRecord(String database, String collection, String fieldRenames, Document snapshot, Document document) throws Exception {
        // Store the snapshot read and start the connector
        final SourceRecord readRecord = getReadRecord(database, collection, fieldRenames, snapshot);

        updateDocument(database, collection, getFilterFromId(snapshot.getObjectId(ID)), document);

        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        assertThat(sourceRecords.allRecordsInOrder().size()).isEqualTo(1);

        return sourceRecords.allRecordsInOrder().get(0);
    }
}
