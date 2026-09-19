/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.data.Envelope.Operation;
import io.debezium.data.Json;
import io.debezium.doc.FixFor;

class ExtractNewDocumentStateResumeTokenTest {

    private static final String TOKEN = "{\"_data\":\"event-token\"}";
    private static final String OUTPUT_FIELD = "__source_resume_token";
    private static final Schema LEGACY_SOURCE_SCHEMA = SchemaBuilder.struct()
            .name("io.debezium.connector.mongo.Source")
            .field("name", Schema.STRING_SCHEMA)
            .build();
    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .name("io.debezium.connector.mongo.Source")
            .field("name", Schema.STRING_SCHEMA)
            .field("resume_token", Json.builder().optional().build())
            .build();

    @ParameterizedTest
    @FixFor("debezium/dbz#718")
    @ValueSource(strings = { "add.fields", "add.headers" })
    void shouldCopyTheCompleteToken(String option) {
        try (var transformation = transformation(option)) {
            final var transformed = transformation.apply(record(SOURCE_SCHEMA, TOKEN, Operation.CREATE));
            assertThat(tokenValue(transformed, option)).isEqualTo(TOKEN);
            assertThat(((Struct) transformed.value()).getInt32("_id")).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#718")
    @ValueSource(strings = { "add.fields", "add.headers" })
    void shouldAcceptNullSnapshotTokens(String option) {
        try (var transformation = transformation(option)) {
            final var transformed = transformation.apply(record(SOURCE_SCHEMA, null, Operation.READ));
            assertThat(tokenValue(transformed, option)).isNull();
            if (option.equals("add.fields")) {
                assertThat(transformed.valueSchema().field(OUTPUT_FIELD).schema().isOptional()).isTrue();
            }
        }
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#718")
    @ValueSource(strings = { "add.fields", "add.headers" })
    void shouldReplayRecordsWhoseSourceSchemaPredatesTheTokenField(String option) {
        try (var transformation = transformation(option)) {
            final var transformed = transformation.apply(record(LEGACY_SOURCE_SCHEMA, null, Operation.CREATE));
            assertThat(((Struct) transformed.value()).getInt32("_id")).isEqualTo(1);
            assertThat(transformed.valueSchema().field(OUTPUT_FIELD)).isNull();
            assertThat(transformed.headers().lastWithName(OUTPUT_FIELD)).isNull();
        }
    }

    @Test
    @FixFor("debezium/dbz#718")
    void shouldResolveAdditionalFieldsForEachRecordSchema() {
        try (var transformation = new ExtractNewDocumentState<SourceRecord>()) {
            transformation.configure(Map.of("add.fields", "source.name,source.resume_token,source.resume_tokne"));

            for (Schema sourceSchema : List.of(LEGACY_SOURCE_SCHEMA, SOURCE_SCHEMA, LEGACY_SOURCE_SCHEMA)) {
                final var transformed = transformation.apply(record(sourceSchema, TOKEN, Operation.CREATE));
                final var value = (Struct) transformed.value();
                assertThat(value.getInt32("_id")).isEqualTo(1);
                assertThat(value.getString("__source_name")).isEqualTo("test");
                assertThat(transformed.valueSchema().field("__source_resume_tokne")).isNull();

                if (sourceSchema.field("resume_token") == null) {
                    assertThat(transformed.valueSchema().field(OUTPUT_FIELD)).isNull();
                }
                else {
                    assertThat(value.getString(OUTPUT_FIELD)).isEqualTo(TOKEN);
                }
            }
        }
    }

    @Test
    @FixFor("debezium/dbz#718")
    void shouldPreserveDocumentFieldsWhenRequestedMetadataIsAbsent() {
        try (var transformation = transformation("add.fields")) {
            final var original = record(LEGACY_SOURCE_SCHEMA, null, Operation.CREATE);
            ((Struct) original.value()).put("after", "{\"_id\":1,\"__source_resume_token\":\"application-value\"}");
            final var transformed = transformation.apply(original);
            assertThat(((Struct) transformed.value()).getString(OUTPUT_FIELD)).isEqualTo("application-value");
        }
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#718")
    @ValueSource(strings = { "add.fields", "add.headers" })
    void shouldRetainTheDeleteTokenWhenRewritingDeletes(String option) {
        try (var transformation = transformation(option)) {
            final var transformed = transformation.apply(record(SOURCE_SCHEMA, TOKEN, Operation.DELETE));
            assertThat(tokenValue(transformed, option)).isEqualTo(TOKEN);
            assertThat(((Struct) transformed.value()).getBoolean("__deleted")).isTrue();
        }
    }

    @Test
    @FixFor("debezium/dbz#718")
    void shouldNotInventATokenForTombstones() {
        try (var transformation = new ExtractNewDocumentState<SourceRecord>()) {
            transformation.configure(Map.of("add.headers", "source.resume_token", "delete.tombstone.handling.mode", "rewrite-with-tombstone"));
            final var record = record(SOURCE_SCHEMA, TOKEN, Operation.DELETE);
            final var tombstone = record.newRecord(record.topic(), null, record.keySchema(), record.key(), null, null, null);
            final var transformed = transformation.apply(tombstone);
            assertThat(transformed.value()).isNull();
            assertThat(transformed.headers().lastWithName(OUTPUT_FIELD)).isNull();
        }
    }

    private static Object tokenValue(SourceRecord record, String option) {
        if (option.equals("add.fields")) {
            return ((Struct) record.value()).getString(OUTPUT_FIELD);
        }
        return record.headers().lastWithName(OUTPUT_FIELD).value();
    }

    private static ExtractNewDocumentState<SourceRecord> transformation(String option) {
        final var transformation = new ExtractNewDocumentState<SourceRecord>();
        transformation.configure(Map.of(option, "source.resume_token", "delete.tombstone.handling.mode", "rewrite"));
        return transformation;
    }

    private static SourceRecord record(Schema sourceSchema, String token, Operation operation) {
        final var source = new Struct(sourceSchema).put("name", "test");
        if (sourceSchema.field("resume_token") != null) {
            source.put("resume_token", token);
        }
        final var schema = SchemaBuilder.struct().name("test.db.names.Envelope")
                .field("source", sourceSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("after", Schema.OPTIONAL_STRING_SCHEMA)
                .field("before", Schema.OPTIONAL_STRING_SCHEMA)
                .field("updateDescription", SchemaBuilder.struct().optional().build())
                .build();
        final var value = new Struct(schema).put("source", source).put("op", operation.code());
        if (operation != Operation.DELETE) {
            value.put("after", "{\"_id\":1,\"name\":\"Alice\"}");
        }
        final var keySchema = SchemaBuilder.struct().name("test.db.names.Key").field("id", Schema.STRING_SCHEMA).build();
        return new SourceRecord(Map.of("server", "test"), Map.of(), "test.db.names", keySchema,
                new Struct(keySchema).put("id", "1"), schema, value);
    }
}
