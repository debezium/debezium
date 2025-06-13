/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.data.VerifyRecord.assertConnectSchemasAreEqual;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.AbstractSourceInfoStructMaker;

/**
 * @author Randall Hauch
 */
public class SourceInfoTest {

    private static final String REPLICA_SET_NAME = "myReplicaSet";

    // For resume token with all fields included
    private static final String CHANGE_RESUME_TOKEN_JSON = "{\n" +
            "    \"_data\": \"82647A41A6000000012B022C0100296E5A10042409C0859BCF45ABBE0E0BD72AB4040346465F696400461E666F6F002B021E626172002B04000004\",\n" +
            "    \"_typeBits\":\n" +
            "    {\n" +
            "        \"$binary\":\n" +
            "        {\n" +
            "            \"base64\": \"gkAB\",\n" +
            "            \"subType\": \"00\"\n" +
            "        }\n" +
            "    }\n" +
            "}";
    private static final BsonDocument CHANGE_RESUME_TOKEN = BsonDocument.parse(CHANGE_RESUME_TOKEN_JSON);
    private static final String CHANGE_RESUME_TOKEN_STRING = ResumeTokens.toBase64(CHANGE_RESUME_TOKEN);
    private static final BsonTimestamp CHANGE_TIMESTAMP = new BsonTimestamp(1666193824, 1);

    // FROM: https://www.mongodb.com/docs/manual/changeStreams/#resume-tokens-from-aggregate
    private static final String CURSOR_RESUME_TOKEN_JSON = "{ \"_data\" : \"8263515EAC000000022B0429296E1404\" }";
    private static final BsonDocument CURSOR_RESUME_TOKEN = BsonDocument.parse(CURSOR_RESUME_TOKEN_JSON);
    private static final String CURSOR_RESUME_TOKEN_STRING = ResumeTokens.toBase64(CURSOR_RESUME_TOKEN);

    private SourceInfo source;
    private MongoDbOffsetContext context;

    @Before
    public void beforeEach() {
        createOffsetContext();
    }

    private SourceInfo createSourceInfo() {
        var config = new MongoDbConnectorConfig(Configuration.create()
                .with(MongoDbConnectorConfig.CONNECTION_STRING, "mongodb://localhost:2017/?replicaSet=" + REPLICA_SET_NAME)
                .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                .build());

        return new SourceInfo(config);
    }

    private void createOffsetContext() {
        var config = new MongoDbConnectorConfig(Configuration.create()
                .with(MongoDbConnectorConfig.CONNECTION_STRING, "mongodb://localhost:2017/?replicaSet=" + REPLICA_SET_NAME)
                .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                .build());

        context = MongoDbOffsetContext.empty(config);
        source = context.sourceInfo();
    }

    @SuppressWarnings("unchecked")
    private MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> mockEventChangeStreamCursor() {
        final var cursor = mock(MongoChangeStreamCursor.class);

        // FROM: https://www.mongodb.com/docs/manual/changeStreams/#resume-tokens-from-change-events
        final var event = new ChangeStreamDocument<BsonDocument>(
                OperationType.INSERT.getValue(), // operation type
                CHANGE_RESUME_TOKEN, // resumeToken
                BsonDocument.parse("{db: \"test\", coll: \"names\"}"), // namespaceDocument
                null, // destinationNamespaceDocument
                null, // fullDocument
                null, // fullDocumentBeforeChange
                new BsonDocument("_id", new BsonObjectId(new ObjectId("635019a078be67426d7cf4d2"))), // documentKey
                CHANGE_TIMESTAMP, // clusterTime
                null, // updateDescription
                null, // txnNumber
                null, // lsid
                null, // wallTime
                null // extraElements
        );

        when(cursor.tryNext()).thenReturn(event);

        return cursor;
    }

    @SuppressWarnings("unchecked")
    private MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> mockNoEventChangeStreamCursor() {
        final var cursor = mock(MongoChangeStreamCursor.class);
        when(cursor.tryNext()).thenReturn(null);
        when(cursor.getResumeToken()).thenReturn(CURSOR_RESUME_TOKEN);

        return cursor;
    }

    public void assertSourceInfoContents(SourceInfo source,
                                         MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
                                         String resumeTokenData,
                                         BsonTimestamp timestamp,
                                         String snapshot) {
        if (cursor != null) {
            assertThat(source.hasPosition()).isEqualTo(false);
            source.initEvent(cursor);
        }

        assertSourceInfoContents(source, cursor != null, resumeTokenData, timestamp, snapshot);
    }

    public void assertSourceInfoContents(SourceInfo source,
                                         boolean hasOffset,
                                         String resumeTokenData,
                                         BsonTimestamp timestamp,
                                         String snapshot) {
        assertThat(source.hasPosition()).isEqualTo(hasOffset);

        Map<String, ?> offset = context.getOffset();
        assertThat(offset.get(SourceInfo.TIMESTAMP)).isEqualTo((timestamp != null) ? timestamp.getTime() : 0);
        assertThat(offset.get(SourceInfo.ORDER)).isEqualTo((timestamp != null) ? timestamp.getInc() : -1);

        String resumeToken = source.lastResumeToken();
        assertThat(resumeToken).isEqualTo(resumeTokenData);

        source.collectionEvent(new CollectionId("test", "names"), 0L);
        Struct struct = source.struct();
        assertThat(struct.getInt64(SourceInfo.TIMESTAMP_KEY)).isEqualTo((timestamp != null) ? timestamp.getTime() * 1000L : 0L);
        assertThat(struct.getInt32(SourceInfo.ORDER)).isEqualTo((timestamp != null) ? timestamp.getInc() : -1);
        assertThat(struct.getString(SourceInfo.DATABASE_NAME_KEY)).isEqualTo("test");
        assertThat(struct.getString(SourceInfo.COLLECTION)).isEqualTo("names");
        assertThat(struct.getString(SourceInfo.SERVER_NAME_KEY)).isEqualTo("serverX");
        assertThat(struct.getString(SourceInfo.SNAPSHOT_KEY)).isEqualTo(snapshot);
    }

    @Test
    public void shouldSetAndReturnRecordedOffset() {
        var cursor = mockEventChangeStreamCursor();
        assertSourceInfoContents(source, cursor, CHANGE_RESUME_TOKEN_STRING, CHANGE_TIMESTAMP, null);

        // Create a new source info and set the offset ...
        var position = source.position();
        SourceInfo newSource = createSourceInfo();
        newSource.setPosition(position);

        assertSourceInfoContents(newSource, true, CHANGE_RESUME_TOKEN_STRING, CHANGE_TIMESTAMP, null);
    }

    @Test
    public void shouldReturnOffsetForUnusedReplicaName() {
        assertThat(source.hasPosition()).isEqualTo(false);
        assertSourceInfoContents(source, false, null, new BsonTimestamp(0), null);
    }

    @Test
    public void shouldReturnRecordedOffsetForUsedReplicaName() {
        var cursor = mockEventChangeStreamCursor();
        assertSourceInfoContents(source, cursor, CHANGE_RESUME_TOKEN_STRING, CHANGE_TIMESTAMP, null);
    }

    @Test
    public void shouldReturnRecordedOffsetForUsedReplicaNameWithoutEvent() {
        var cursor = mockNoEventChangeStreamCursor();
        assertSourceInfoContents(source, cursor, CURSOR_RESUME_TOKEN_STRING, null, null);
    }

    @Test
    public void shouldReturnOffsetForUnusedReplicaNameDuringInitialSnapshot() {
        source.startInitialSnapshot();
        assertSourceInfoContents(source, false, null, new BsonTimestamp(0), "true");
    }

    @Test
    public void shouldReturnRecordedOffsetForUsedReplicaNameDuringInitialSnapshot() {
        source.startInitialSnapshot();

        var cursor = mockEventChangeStreamCursor();
        assertSourceInfoContents(source, cursor, CHANGE_RESUME_TOKEN_STRING, CHANGE_TIMESTAMP, "true");
    }

    @Test
    public void shouldReturnRecordedOffsetForUsedReplicaNameDuringInitialSnapshotWithoutEvent() {
        source.startInitialSnapshot();

        var cursor = mockNoEventChangeStreamCursor();
        assertSourceInfoContents(source, cursor, CURSOR_RESUME_TOKEN_STRING, null, "true");
    }

    @Test
    public void versionIsPresent() {
        var cursor = mockEventChangeStreamCursor();
        source.initEvent(cursor);
        assertThat(source.struct().getString(SourceInfo.DEBEZIUM_VERSION_KEY)).isEqualTo(Module.version());
    }

    @Test
    public void connectorIsPresent() {
        var cursor = mockEventChangeStreamCursor();
        source.initEvent(cursor);
        assertThat(source.struct().getString(SourceInfo.DEBEZIUM_CONNECTOR_KEY)).isEqualTo(Module.name());
    }

    @Test
    public void wallTimeIsPresent() {
        var cursor = mockEventChangeStreamCursor();
        source.initEvent(cursor);
        assertThat(source.struct().getInt64(SourceInfo.WALL_TIME)).isNull();
        source.collectionEvent(new CollectionId("test", "names"), 10L);
        assertThat(source.struct().getInt64(SourceInfo.WALL_TIME)).isEqualTo(10L);
    }

    @Test
    public void shouldHaveSchemaForSource() {
        Schema schema = context.getSourceInfoSchema();
        assertThat(schema.name()).isNotEmpty();
        assertThat(schema.version()).isNull();
        assertThat(schema.field(SourceInfo.SERVER_NAME_KEY).schema()).isEqualTo(Schema.STRING_SCHEMA);
        assertThat(schema.field(SourceInfo.DATABASE_NAME_KEY).schema()).isEqualTo(Schema.STRING_SCHEMA);
        assertThat(schema.field(SourceInfo.COLLECTION).schema()).isEqualTo(Schema.STRING_SCHEMA);
        assertThat(schema.field(SourceInfo.TIMESTAMP_KEY).schema()).isEqualTo(Schema.INT64_SCHEMA);
        assertThat(schema.field(SourceInfo.ORDER).schema()).isEqualTo(Schema.INT32_SCHEMA);
        assertThat(schema.field(SourceInfo.SNAPSHOT_KEY).schema()).isEqualTo(AbstractSourceInfoStructMaker.SNAPSHOT_RECORD_SCHEMA);
    }

    @Test
    public void schemaIsCorrect() {
        final Schema schema = SchemaBuilder.struct()
                .name("io.debezium.connector.mongo.Source")
                .field("version", Schema.STRING_SCHEMA)
                .field("connector", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .field("snapshot", AbstractSourceInfoStructMaker.SNAPSHOT_RECORD_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field("sequence", Schema.OPTIONAL_STRING_SCHEMA)
                .field("collection", Schema.STRING_SCHEMA)
                .field("ord", Schema.INT32_SCHEMA)
                .field("lsid", Schema.OPTIONAL_STRING_SCHEMA)
                .field("txnNumber", Schema.OPTIONAL_INT64_SCHEMA)
                .field("wallTime", Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        assertConnectSchemasAreEqual(null, source.schema(), schema);
    }
}
