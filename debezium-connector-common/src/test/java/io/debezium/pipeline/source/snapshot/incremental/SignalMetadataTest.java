/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SignalMetadataTest {

    private static final Instant OPEN = Instant.parse("2026-01-01T00:00:00Z");
    private static final Instant CLOSE = Instant.parse("2026-01-01T00:00:05Z");

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldKeepLegacyFormatWhenNoAttributionIsSet() {
        assertThat(new SignalMetadata(OPEN, null).metadataString())
                .isEqualTo("{\"openWindowTimestamp\": \"2026-01-01T00:00:00Z\"}");
        assertThat(new SignalMetadata(OPEN, CLOSE).metadataString())
                .isEqualTo("{\"openWindowTimestamp\": \"2026-01-01T00:00:00Z\", \"closeWindowTimestamp\": \"2026-01-01T00:00:05Z\"}");
    }

    @Test
    public void shouldEmitCorrelationIdAndDataCollectionId() throws Exception {
        final SignalMetadata metadata = new SignalMetadata(OPEN, CLOSE, "signal-1", "public.my_table");

        final JsonNode json = mapper.readTree(metadata.metadataString());
        assertThat(json.get("openWindowTimestamp").asText()).isEqualTo("2026-01-01T00:00:00Z");
        assertThat(json.get("closeWindowTimestamp").asText()).isEqualTo("2026-01-01T00:00:05Z");
        assertThat(json.get("correlationId").asText()).isEqualTo("signal-1");
        assertThat(json.get("dataCollectionId").asText()).isEqualTo("public.my_table");
    }

    @Test
    public void shouldOmitAbsentFields() throws Exception {
        final SignalMetadata metadata = new SignalMetadata(OPEN, null, null, "public.my_table");

        final JsonNode json = mapper.readTree(metadata.metadataString());
        assertThat(json.has("closeWindowTimestamp")).isFalse();
        assertThat(json.has("correlationId")).isFalse();
        assertThat(json.get("dataCollectionId").asText()).isEqualTo("public.my_table");
    }

    @Test
    public void shouldEscapeJsonSpecialCharacters() throws Exception {
        final SignalMetadata metadata = new SignalMetadata(OPEN, null, "id-with-\"quote\"", "schema.table\\odd");

        final JsonNode json = mapper.readTree(metadata.metadataString());
        assertThat(json.get("correlationId").asText()).isEqualTo("id-with-\"quote\"");
        assertThat(json.get("dataCollectionId").asText()).isEqualTo("schema.table\\odd");
    }

    @Test
    public void shouldEscapeControlCharacters() throws Exception {
        final SignalMetadata metadata = new SignalMetadata(OPEN, null, "id\nwith\tcontrols\r", "schema.table\fodd\b");

        final JsonNode json = mapper.readTree(metadata.metadataString());
        assertThat(json.get("correlationId").asText()).isEqualTo("id\nwith\tcontrols\r");
        assertThat(json.get("dataCollectionId").asText()).isEqualTo("schema.table\fodd\b");
    }

    @Test
    public void shouldCopyAllFieldsWhenClosingWindow() {
        final SignalMetadata closed = new SignalMetadata(OPEN, null, "signal-1", "public.my_table").withCloseWindowTimestamp(CLOSE);

        assertThat(closed.getOpenWindowTimestamp()).isEqualTo(OPEN);
        assertThat(closed.getCloseWindowTimestamp()).isEqualTo(CLOSE);
        assertThat(closed.getCorrelationId()).isEqualTo("signal-1");
        assertThat(closed.getDataCollectionId()).isEqualTo("public.my_table");
    }
}
