/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.time.Instant;

/**
 * Signal metadata for the incremental snapshotting
 *
 * @author Anisha Mohanty
 */
public class SignalMetadata {
    private final Instant openWindowTimestamp;
    private final Instant closeWindowTimestamp;

    public SignalMetadata(Instant openWindowTimestamp, Instant closeWindowTimestamp) {
        this.openWindowTimestamp = openWindowTimestamp;
        this.closeWindowTimestamp = closeWindowTimestamp;
    }

    public String metadataString() {
        if (closeWindowTimestamp == null) {
            return String.format("{\"openWindowTimestamp\": \"%s\"}", openWindowTimestamp);
        }
        return String.format("{\"openWindowTimestamp\": \"%s\", \"closeWindowTimestamp\": \"%s\"}", openWindowTimestamp, closeWindowTimestamp);
    }

    public Instant getOpenWindowTimestamp() {
        return openWindowTimestamp;
    }

    public Instant getCloseWindowTimestamp() {
        return closeWindowTimestamp;
    }
}
