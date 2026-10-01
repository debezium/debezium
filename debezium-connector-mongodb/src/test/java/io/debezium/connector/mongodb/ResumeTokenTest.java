/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.codecs.DecoderContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.JsonSerializationMode;
import io.debezium.connector.mongodb.events.BufferingChangeStreamCursor.ResumableChangeStreamEvent;
import io.debezium.connector.mongodb.events.SplitEventHandler;
import io.debezium.data.Json;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;

class ResumeTokenTest {

    private static final BsonDocument TOKEN = BsonDocument.parse("""
            {
              "_data": "82647A41A6000000012B022C0100296E5A10042409C0859BCF45ABBE0E0BD72AB4040346465F696400461E666F6F002B021E626172002B04000004",
              "_typeBits": {"$binary": {"base64": "gkAB", "subType": "00"}}
            }
            """);
    private static final CollectionId COLLECTION = new CollectionId("test", "names");

    @Test
    @FixFor("debezium/dbz#718")
    void shouldDeclareAnOptionalJsonTokenWithoutAnInitialValue() {
        final var context = context(JsonSerializationMode.LEGACY);
        final var field = context.getSourceInfoSchema().field(SourceInfo.RESUME_TOKEN);

        assertThat(field).isNotNull();
        assertThat(field.schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(field.schema().isOptional()).isTrue();
        assertThat(field.schema().name()).isEqualTo(Json.LOGICAL_NAME);
        assertThat(context.getSourceInfo().getString(SourceInfo.RESUME_TOKEN)).isNull();
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#718")
    @EnumSource(JsonSerializationMode.class)
    void shouldPreserveCompleteCrudTokensUsingTheConfiguredJsonMode(JsonSerializationMode mode) {
        final var context = context(mode);
        for (var operation : List.of(OperationType.INSERT, OperationType.UPDATE, OperationType.REPLACE, OperationType.DELETE)) {
            context.changeStreamEvent(event(operation, TOKEN));

            final var json = context.getSourceInfo().getString(SourceInfo.RESUME_TOKEN);
            assertThat(BsonDocument.parse(json)).isEqualTo(TOKEN);
            assertThat(context.getOffset().get(SourceInfo.RESUME_TOKEN)).isEqualTo(ResumeTokens.toBase64(TOKEN));
            if (mode == JsonSerializationMode.LEGACY || mode == JsonSerializationMode.STRICT) {
                assertThat(json).contains("\"$type\": \"00\"").doesNotContain("\"subType\"");
            }
            else {
                assertThat(json).contains("\"subType\": \"00\"").doesNotContain("\"$type\"");
            }
        }
    }

    @Test
    @FixFor("debezium/dbz#718")
    void shouldClearTheEventTokenWithoutClearingTheSnapshotOffset() {
        final var context = context(JsonSerializationMode.LEGACY);
        context.changeStreamEvent(event(OperationType.INSERT, TOKEN));
        final var streamingSource = context.getSourceInfo();
        final var offset = context.getOffset();

        context.sourceInfo().startInitialSnapshot();
        assertThat(context.getSourceInfo().getString(SourceInfo.RESUME_TOKEN)).isNull();
        context.readEvent(COLLECTION, null);
        context.sourceInfo().stopInitialSnapshot();

        assertThat(context.getSourceInfo().getString(SourceInfo.RESUME_TOKEN)).isNull();
        assertThat(context.getOffset()).isEqualTo(offset);
        assertThat(BsonDocument.parse(streamingSource.getString(SourceInfo.RESUME_TOKEN))).isEqualTo(TOKEN);
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#718")
    @ValueSource(booleans = { false, true })
    void shouldNotLeakTokensAcrossSnapshotReads(boolean blocking) {
        final var context = context(JsonSerializationMode.LEGACY);
        context.changeStreamEvent(event(OperationType.UPDATE, TOKEN));
        context.preSnapshotStart(blocking);
        context.readEvent(COLLECTION, null);
        assertThat(context.getSourceInfo().getString(SourceInfo.RESUME_TOKEN)).isNull();
        context.postSnapshotCompletion();
        assertThat(context.getSourceInfo().getString(SourceInfo.RESUME_TOKEN)).isNull();

        final var nextToken = new BsonDocument("_data", new BsonString("next-event"));
        context.changeStreamEvent(event(OperationType.DELETE, nextToken));
        assertThat(BsonDocument.parse(context.getSourceInfo().getString(SourceInfo.RESUME_TOKEN))).isEqualTo(nextToken);
    }

    @Test
    @FixFor("debezium/dbz#718")
    void shouldKeepStreamingRecordsIndependentOfIncrementalSnapshotState() {
        final var context = context(JsonSerializationMode.EXTENDED);
        context.changeStreamEvent(event(OperationType.UPDATE, TOKEN));
        final var streamingSource = context.getSourceInfo();

        context.incrementalSnapshotEvents();
        context.readEvent(COLLECTION, null);
        assertThat(context.getSourceInfo().getString(SourceInfo.RESUME_TOKEN)).isNull();
        assertThat(context.lastResumeTokenDoc()).isEqualTo(TOKEN);
        context.postSnapshotCompletion();
        assertThat(context.getSourceInfo().getString(SourceInfo.RESUME_TOKEN)).isNull();

        final var nextToken = new BsonDocument("_data", new BsonString("after-window"));
        context.changeStreamEvent(event(OperationType.UPDATE, nextToken));
        assertThat(BsonDocument.parse(context.getSourceInfo().getString(SourceInfo.RESUME_TOKEN))).isEqualTo(nextToken);
        assertThat(BsonDocument.parse(streamingSource.getString(SourceInfo.RESUME_TOKEN))).isEqualTo(TOKEN);
    }

    @Test
    @FixFor("debezium/dbz#718")
    void shouldNotExposeRestoredOffsetsAsDataEventTokens() {
        final var context = context(JsonSerializationMode.LEGACY);
        context.changeStreamEvent(event(OperationType.INSERT, TOKEN));
        final var config = config(JsonSerializationMode.LEGACY);
        final var restored = new MongoDbOffsetContext.Loader(config).load(context.getOffset());
        assertThat(restored.lastResumeTokenDoc()).isEqualTo(TOKEN);
        assertThat(restored.getSourceInfo().getString(SourceInfo.RESUME_TOKEN)).isNull();

        context.sourceInfo().setPosition(context.sourceInfo().position());
        assertThat(context.getSourceInfo().getString(SourceInfo.RESUME_TOKEN)).isNull();
        assertThat(context.lastResumeTokenDoc()).isEqualTo(TOKEN);
    }

    @Test
    @FixFor("debezium/dbz#718")
    void shouldNotExposeHeartbeatTokensAsDataEventTokens() {
        final var context = context(JsonSerializationMode.LEGACY);
        context.changeStreamEvent(event(OperationType.INSERT, TOKEN));
        final var watermark = new BsonDocument("_data", new BsonString("watermark"));
        context.noEvent(new ResumableChangeStreamEvent<>(watermark));
        assertThat(context.getSourceInfo().getString(SourceInfo.RESUME_TOKEN)).isNull();
        assertThat(context.lastResumeTokenDoc()).isEqualTo(watermark);
    }

    @Test
    @FixFor("debezium/dbz#718")
    void shouldNotExposeATokenForAnOperationTimePosition() {
        final var context = context(JsonSerializationMode.LEGACY);
        context.changeStreamEvent(event(OperationType.INSERT, TOKEN));
        context.sourceInfo().noEvent(new BsonTimestamp(100, 1));
        assertThat(context.getSourceInfo().getString(SourceInfo.RESUME_TOKEN)).isNull();
    }

    @Test
    @FixFor("debezium/dbz#718")
    void shouldClearTheTokenWhenTheChangeStreamEventIsNull() {
        final var context = context(JsonSerializationMode.LEGACY);
        context.changeStreamEvent(event(OperationType.INSERT, TOKEN));
        context.changeStreamEvent(null);
        assertThat(context.getSourceInfo().getString(SourceInfo.RESUME_TOKEN)).isNull();
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#718")
    @ValueSource(ints = { 2, 3 })
    void shouldExposeOnlyTheFinalFragmentToken(int fragmentCount) {
        final var handler = new SplitEventHandler<BsonDocument>();
        final var context = context(JsonSerializationMode.LEGACY);
        for (var fragmentNumber = 1; fragmentNumber <= fragmentCount; fragmentNumber++) {
            final var token = new BsonDocument("_data", new BsonString("fragment-" + fragmentNumber));
            final var document = fragmentNumber == 1 ? eventDocument(OperationType.UPDATE, token) : new BsonDocument("_id", token);
            final var splitEvent = new BsonDocument("fragment", new BsonInt32(fragmentNumber))
                    .append("of", new BsonInt32(fragmentCount));
            document.append("splitEvent", splitEvent);

            final var result = handler.handle(decode(document));
            if (fragmentNumber < fragmentCount) {
                assertThat(result).isEmpty();
                continue;
            }

            assertThat(result).isPresent();
            final var completeEvent = result.orElseThrow();
            assertThat(completeEvent.getSplitEvent()).isNull();
            context.changeStreamEvent(completeEvent);
            assertThat(BsonDocument.parse(context.getSourceInfo().getString(SourceInfo.RESUME_TOKEN))).isEqualTo(token);
            assertThat(context.lastResumeTokenDoc()).isEqualTo(token);
        }
        assertThat(handler.isEmpty()).isTrue();
        context.changeStreamEvent(handler.handle(event(OperationType.INSERT, TOKEN)).orElseThrow());
        assertThat(BsonDocument.parse(context.getSourceInfo().getString(SourceInfo.RESUME_TOKEN))).isEqualTo(TOKEN);
    }

    @Test
    @FixFor("debezium/dbz#718")
    void shouldResumeFromLegacyOffsetsWithoutExposingThemAsEventTokens() {
        final var legacyToken = TOKEN.getString("_data").getValue();
        final var context = new MongoDbOffsetContext.Loader(config(JsonSerializationMode.EXTENDED))
                .load(Map.of(SourceInfo.RESUME_TOKEN, legacyToken, SourceInfo.TIMESTAMP, 100, SourceInfo.ORDER, 1));

        assertThat(context.lastResumeTokenDoc()).isEqualTo(new BsonDocument("_data", new BsonString(legacyToken)));
        assertThat(context.getSourceInfo().getString(SourceInfo.RESUME_TOKEN)).isNull();
        context.changeStreamEvent(event(OperationType.UPDATE, TOKEN));
        assertThat(BsonDocument.parse(context.getSourceInfo().getString(SourceInfo.RESUME_TOKEN))).isEqualTo(TOKEN);
        assertThat(context.getOffset().get(SourceInfo.RESUME_TOKEN)).isEqualTo(ResumeTokens.toBase64(TOKEN));
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#718")
    @EnumSource(JsonSerializationMode.class)
    void shouldRoundTripSourceMetadataThroughJsonAndAvro(JsonSerializationMode mode) {
        final var context = context(mode);
        context.changeStreamEvent(event(OperationType.INSERT, TOKEN));
        assertThat(context.getSourceInfoSchema().field(SourceInfo.RESUME_TOKEN)).isNotNull();
        verifyConverters(context);
        context.readEvent(COLLECTION, null);
        verifyConverters(context);
    }

    private static void verifyConverters(MongoDbOffsetContext context) {
        final var source = context.getSourceInfo();
        final var keySchema = SchemaBuilder.struct().name("tokens.Key").field("id", Schema.STRING_SCHEMA).build();
        final var key = new Struct(keySchema).put("id", "key");
        VerifyRecord.isValid(new SourceRecord(Map.of("server", "test"), context.getOffset(), "tokens",
                keySchema, key, source.schema(), source));
    }

    private static MongoDbOffsetContext context(JsonSerializationMode mode) {
        return MongoDbOffsetContext.empty(config(mode));
    }

    private static MongoDbConnectorConfig config(JsonSerializationMode mode) {
        return new MongoDbConnectorConfig(Configuration.create()
                .with(MongoDbConnectorConfig.CONNECTION_STRING, "mongodb://localhost:27017")
                .with(MongoDbConnectorConfig.TOPIC_PREFIX, "test")
                .with(MongoDbConnectorConfig.JSON_SERIALIZATION_MODE, mode)
                .build());
    }

    private static ChangeStreamDocument<BsonDocument> event(OperationType operation, BsonDocument token) {
        return decode(eventDocument(operation, token));
    }

    private static BsonDocument eventDocument(OperationType operation, BsonDocument token) {
        return BsonDocument.parse("{ns:{db:'test',coll:'names'},documentKey:{_id:1}}")
                .append("_id", token)
                .append("operationType", new BsonString(operation.getValue()))
                .append("clusterTime", new BsonTimestamp(100, 1));
    }

    private static ChangeStreamDocument<BsonDocument> decode(BsonDocument document) {
        return ChangeStreamDocument.createCodec(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry())
                .decode(new BsonDocumentReader(document), DecoderContext.builder().build());
    }
}
