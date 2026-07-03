/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Date;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.doc.FixFor;

public class JsonSerializationModeIT extends AbstractMongoConnectorIT {

    private static final String DATABASE_NAME = "dbJson";
    private static final String COLLECTION_NAME = "c1";
    private static final String SERVER_NAME = "serverX";
    private static final long CREATE_DATE = 1_783_078_553_473L;
    private static final long UPDATE_DATE = 1_783_078_851_680L;

    @Test
    @FixFor("dbz#731")
    void shouldSerializeEventPayloadsUsingDefaultLegacyJsonMode() throws Exception {
        assertJsonMode(null,
                "\"phone\": {\"$numberLong\": \"123\"}",
                "\"created\": {\"$date\": " + CREATE_DATE + "}",
                "\"phone\": 456",
                "\"created\": {\"$date\": \"");
    }

    @Test
    @FixFor("dbz#731")
    void shouldSerializeEventPayloadsUsingStrictJsonMode() throws Exception {
        assertJsonMode(MongoDbConnectorConfig.JsonSerializationMode.STRICT,
                "\"phone\": {\"$numberLong\": \"123\"}",
                "\"created\": {\"$date\": " + CREATE_DATE + "}",
                "\"phone\": {\"$numberLong\": \"456\"}",
                "\"created\": {\"$date\": " + UPDATE_DATE + "}");
    }

    @Test
    @FixFor("dbz#731")
    void shouldSerializeEventPayloadsUsingExtendedJsonMode() throws Exception {
        assertJsonMode(MongoDbConnectorConfig.JsonSerializationMode.EXTENDED,
                "\"phone\": {\"$numberLong\": \"123\"}",
                "\"created\": {\"$date\": {\"$numberLong\": \"" + CREATE_DATE + "\"}}",
                "\"phone\": {\"$numberLong\": \"456\"}",
                "\"created\": {\"$date\": {\"$numberLong\": \"" + UPDATE_DATE + "\"}}");
    }

    @Test
    @FixFor("dbz#731")
    void shouldSerializeEventPayloadsUsingRelaxedJsonMode() throws Exception {
        assertJsonMode(MongoDbConnectorConfig.JsonSerializationMode.RELAXED,
                "\"phone\": 123",
                "\"created\": {\"$date\": \"",
                "\"phone\": 456",
                "\"created\": {\"$date\": \"");
    }

    private void assertJsonMode(MongoDbConnectorConfig.JsonSerializationMode mode, String expectedAfterPhone, String expectedAfterCreated,
                                String expectedUpdatedFieldsPhone, String expectedUpdatedFieldsCreated)
            throws Exception {
        final Configuration.Builder builder = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, DATABASE_NAME + "." + COLLECTION_NAME)
                .with(CommonConnectorConfig.TOPIC_PREFIX, SERVER_NAME);
        if (mode != null) {
            builder.with(MongoDbConnectorConfig.JSON_SERIALIZATION_MODE, mode);
        }
        config = builder.build();
        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, DATABASE_NAME);

        start(MongoDbConnector.class, config);
        waitForSnapshotToBeCompleted("mongodb", SERVER_NAME);
        waitForStreamingRunning("mongodb", SERVER_NAME);

        final ObjectId objectId = new ObjectId();
        insertDocuments(DATABASE_NAME, COLLECTION_NAME, new Document()
                .append("_id", objectId)
                .append("phone", 123L)
                .append("created", new Date(CREATE_DATE)));

        final SourceRecord createRecord = consumeRecordsByTopic(1).allRecordsInOrder().get(0);
        final Struct createValue = (Struct) createRecord.value();
        final String after = createValue.getString(Envelope.FieldName.AFTER);

        assertThat(createValue.getString(Envelope.FieldName.OPERATION)).isEqualTo(Operation.CREATE.code());
        assertThat(after).contains(expectedAfterPhone);
        assertThat(after).contains(expectedAfterCreated);

        updateDocument(DATABASE_NAME, COLLECTION_NAME,
                new Document("_id", objectId),
                new Document("$set", new Document()
                        .append("phone", 456L)
                        .append("created", new Date(UPDATE_DATE))));

        final SourceRecord updateRecord = consumeRecordsByTopic(1).allRecordsInOrder().get(0);
        final Struct updateValue = (Struct) updateRecord.value();
        final Struct updateDescription = updateValue.getStruct(MongoDbFieldName.UPDATE_DESCRIPTION);
        final String updatedFields = updateDescription.getString(MongoDbFieldName.UPDATED_FIELDS);

        assertThat(updateValue.getString(Envelope.FieldName.OPERATION)).isEqualTo(Operation.UPDATE.code());
        assertThat(updatedFields).contains(expectedUpdatedFieldsPhone);
        assertThat(updatedFields).contains(expectedUpdatedFieldsCreated);
        assertThat(updateDescription.getArray(MongoDbFieldName.REMOVED_FIELDS)).isNull();
    }
}
