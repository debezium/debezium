/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

public class SignalMetadata {
    private final String openWindowTimestamp;
    private final String closeWindowTimestamp;

    public SignalMetadata(String openWindowTimestamp, String closeWindowTimestamp) {
        this.openWindowTimestamp = openWindowTimestamp;
        this.closeWindowTimestamp = closeWindowTimestamp;
    }

    public String openWindowSignalMetadataString() {
        return String.format("{\"openWindowTimestamp\": \"%s\"}", openWindowTimestamp);
    }

    public String closeWindowSignalMetadataString() {
        return String.format("{\"openWindowTimestamp\": \"%s\", \"closeWindowTimestamp\": \"%s\"}", openWindowTimestamp, closeWindowTimestamp);
    }

    public String getOpenWindowTimestamp() {
        return openWindowTimestamp;
    }

    public String getCloseWindowTimestamp() {
        return closeWindowTimestamp;
    }
}
