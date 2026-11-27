/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.api;

/**
 * Token usage of a single evaluation.
 *
 * @param inputTokens number of consumed input tokens
 * @param outputTokens number of produced output tokens
 *
 * @author Jiri Pechanec
 */
public record Usage(long inputTokens, long outputTokens) {

    public Usage {
        if (inputTokens < 0 || outputTokens < 0) {
            throw new IllegalArgumentException("Token counts must not be negative");
        }
    }
}
