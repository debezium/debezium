/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

public class SignalMetadata {
    public enum SignalType {
        OPEN,
        CLOSE
    }

    private final String openWindowTimestamp;
    private final String closeWindowTimestamp;

    public SignalMetadata(String openWindowTimestamp, String closeWindowTimestamp) {
        this.openWindowTimestamp = openWindowTimestamp;
        this.closeWindowTimestamp = closeWindowTimestamp;
    }

    public String signalMetadataString(SignalType type) {
        if (type == SignalType.OPEN) {
            return String.format("{\"openWindowTimestamp\": \"%s\"}", openWindowTimestamp);
        }
        return String.format("{\"openWindowTimestamp\": \"%s\", \"closeWindowTimestamp\": \"%s\"}", openWindowTimestamp, closeWindowTimestamp);
    }

    public String getOpenWindowTimestamp() {
        return openWindowTimestamp;
    }

    public String getCloseWindowTimestamp() {
        return closeWindowTimestamp;
    }
}
