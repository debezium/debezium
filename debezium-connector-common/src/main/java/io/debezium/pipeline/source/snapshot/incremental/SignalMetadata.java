/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import io.debezium.DebeziumException;

/**
 * Signal metadata for the incremental snapshotting
 *
 * @author Anisha Mohanty
 */
public class SignalMetadata {

    /**
     * Writes the metadata JSON with a space after {@code :} and {@code ,}, matching the format
     * emitted by previous versions, with Jackson handling the escaping of field values.
     */
    private static final ObjectWriter WRITER = new ObjectMapper().writer(new LegacySpacingPrettyPrinter());

    private final Instant openWindowTimestamp;
    private final Instant closeWindowTimestamp;

    /**
     * The id of the signal that requested the snapshot of the data collection this window belongs to,
     * so downstream consumers of the watermark events can attribute a window to the snapshot request
     * that caused it. May be {@code null} for snapshots resumed from offsets written by older versions.
     */
    private final String correlationId;

    /**
     * The fully-qualified identifier of the data collection this window's chunk belongs to.
     */
    private final String dataCollectionId;

    public SignalMetadata(Instant openWindowTimestamp, Instant closeWindowTimestamp) {
        this(openWindowTimestamp, closeWindowTimestamp, null, null);
    }

    public SignalMetadata(Instant openWindowTimestamp, Instant closeWindowTimestamp, String correlationId, String dataCollectionId) {
        this.openWindowTimestamp = openWindowTimestamp;
        this.closeWindowTimestamp = closeWindowTimestamp;
        this.correlationId = correlationId;
        this.dataCollectionId = dataCollectionId;
    }

    /**
     * Returns a copy of this metadata with the given close-window timestamp, carrying every other field forward.
     */
    public SignalMetadata withCloseWindowTimestamp(Instant closeWindowTimestamp) {
        return new SignalMetadata(openWindowTimestamp, closeWindowTimestamp, correlationId, dataCollectionId);
    }

    public String metadataString() {
        final Map<String, String> fields = new LinkedHashMap<>();
        if (openWindowTimestamp != null) {
            fields.put("openWindowTimestamp", openWindowTimestamp.toString());
        }
        if (closeWindowTimestamp != null) {
            fields.put("closeWindowTimestamp", closeWindowTimestamp.toString());
        }
        if (correlationId != null) {
            fields.put("correlationId", correlationId);
        }
        if (dataCollectionId != null) {
            fields.put("dataCollectionId", dataCollectionId);
        }
        try {
            return WRITER.writeValueAsString(fields);
        }
        catch (JsonProcessingException e) {
            // Not reachable for a flat map of non-null strings; declared because the exception is checked
            throw new DebeziumException("Cannot serialize signal metadata", e);
        }
    }

    public Instant getOpenWindowTimestamp() {
        return openWindowTimestamp;
    }

    public Instant getCloseWindowTimestamp() {
        return closeWindowTimestamp;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getDataCollectionId() {
        return dataCollectionId;
    }

    private static class LegacySpacingPrettyPrinter extends MinimalPrettyPrinter {

        @Override
        public void writeObjectFieldValueSeparator(JsonGenerator generator) throws IOException {
            generator.writeRaw(": ");
        }

        @Override
        public void writeObjectEntrySeparator(JsonGenerator generator) throws IOException {
            generator.writeRaw(", ");
        }
    }
}
