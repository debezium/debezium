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

    // FROM: https://www.mongodb.com/docs/manual/changeStreams/#resume-tokens-from-change-events
    private static final String CHANGE_RESUME_TOKEN_DATA = "82635019A0000000012B042C0100296E5A1004AB1154ACA" +
            "CD849A48C61756D70D3B21F463C6F7065726174696F6E54" +
            "797065003C696E736572740046646F63756D656E744B657" +
            "90046645F69640064635019A078BE67426D7CF4D2000004";
    private static final BsonDocument CHANGE_RESUME_TOKEN = ResumeTokens.fromData(CHANGE_RESUME_TOKEN_DATA);
    private static final BsonTimestamp CHANGE_TIMESTAMP = new BsonTimestamp(1666193824, 1);

    // FROM: https://www.mongodb.com/docs/manual/changeStreams/#resume-tokens-from-aggregate
    private static final String CURSOR_RESUME_TOKEN_DATA = "8263515EAC000000022B0429296E1404";
    private static final BsonDocument CURSOR_RESUME_TOKEN = ResumeTokens.fromData(CURSOR_RESUME_TOKEN_DATA);

    private SourceInfo source;
    private MongoDbOffsetContext context;

    @Before
    public void beforeEach() {
        createOffsetContext();
    }

    private SourceInfo createSourceInfo() {
        var config = new MongoDbConnectorConfig(Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                .build());

        return new SourceInfo(config, REPLICA_SET_NAME);
    }

    private void createOffsetContext() {
        var config = new MongoDbConnectorConfig(Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                .build());

        context = MongoDbOffsetContext.empty(config, REPLICA_SET_NAME);
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

        source.collectionEvent(new CollectionId(REPLICA_SET_NAME, "test", "names"), 0L);
        Struct struct = source.struct();
        assertThat(struct.getInt64(SourceInfo.TIMESTAMP_KEY)).isEqualTo((timestamp != null) ? timestamp.getTime() * 1000L : 0L);
        assertThat(struct.getInt32(SourceInfo.ORDER)).isEqualTo((timestamp != null) ? timestamp.getInc() : -1);
        assertThat(struct.getString(SourceInfo.DATABASE_NAME_KEY)).isEqualTo("test");
        assertThat(struct.getString(SourceInfo.COLLECTION)).isEqualTo("names");
        assertThat(struct.getString(SourceInfo.REPLICA_SET_NAME)).isEqualTo(REPLICA_SET_NAME);
        assertThat(struct.getString(SourceInfo.SERVER_NAME_KEY)).isEqualTo("serverX");
        assertThat(struct.getString(SourceInfo.SNAPSHOT_KEY)).isEqualTo(snapshot);
    }

    @Test
    public void shouldSetAndReturnRecordedOffset() {
        var cursor = mockEventChangeStreamCursor();
        assertSourceInfoContents(source, cursor, CHANGE_RESUME_TOKEN_DATA, CHANGE_TIMESTAMP, null);

        // Create a new source info and set the offset ...
        var position = source.position();
        SourceInfo newSource = createSourceInfo();
        newSource.setPosition(position);

        assertSourceInfoContents(newSource, true, CHANGE_RESUME_TOKEN_DATA, CHANGE_TIMESTAMP, null);
    }

    @Test
    public void shouldReturnOffsetForUnusedReplicaName() {
        assertThat(source.hasPosition()).isEqualTo(false);
        assertSourceInfoContents(source, false, null, new BsonTimestamp(0), null);
    }

    @Test
    public void shouldReturnRecordedOffsetForUsedReplicaName() {
        var cursor = mockEventChangeStreamCursor();
        assertSourceInfoContents(source, cursor, CHANGE_RESUME_TOKEN_DATA, CHANGE_TIMESTAMP, null);
    }

    @Test
    public void shouldReturnRecordedOffsetForUsedReplicaNameWithoutEvent() {
        var cursor = mockNoEventChangeStreamCursor();
        assertSourceInfoContents(source, cursor, CURSOR_RESUME_TOKEN_DATA, null, null);
    }

    @Test
    public void shouldReturnOffsetForUnusedReplicaNameDuringInitialSync() {
        source.startInitialSync();
        assertSourceInfoContents(source, false, null, new BsonTimestamp(0), "true");
    }

    @Test
    public void shouldReturnRecordedOffsetForUsedReplicaNameDuringInitialSync() {
        source.startInitialSync();

        var cursor = mockEventChangeStreamCursor();
        assertSourceInfoContents(source, cursor, CHANGE_RESUME_TOKEN_DATA, CHANGE_TIMESTAMP, "true");
    }

    @Test
    public void shouldReturnRecordedOffsetForUsedReplicaNameDuringInitialSyncWithoutEvent() {
        source.startInitialSync();

        var cursor = mockNoEventChangeStreamCursor();
        assertSourceInfoContents(source, cursor, CURSOR_RESUME_TOKEN_DATA, null, "true");
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
        source.collectionEvent(new CollectionId(REPLICA_SET_NAME, "test", "names"), 10L);
        assertThat(source.struct().getInt64(SourceInfo.WALL_TIME)).isEqualTo(10L);
    }

    @Test
    public void shouldHaveSchemaForSource() {
        Schema schema = context.getSourceInfoSchema();
        assertThat(schema.name()).isNotEmpty();
        assertThat(schema.version()).isNull();
        assertThat(schema.field(SourceInfo.SERVER_NAME_KEY).schema()).isEqualTo(Schema.STRING_SCHEMA);
        assertThat(schema.field(SourceInfo.REPLICA_SET_NAME).schema()).isEqualTo(Schema.STRING_SCHEMA);
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
                .field("rs", Schema.STRING_SCHEMA)
                .field("collection", Schema.STRING_SCHEMA)
                .field("ord", Schema.INT32_SCHEMA)
                .field("lsid", Schema.OPTIONAL_STRING_SCHEMA)
                .field("txnNumber", Schema.OPTIONAL_INT64_SCHEMA)
                .field("wallTime", Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        assertConnectSchemasAreEqual(null, source.schema(), schema);
    }
}
