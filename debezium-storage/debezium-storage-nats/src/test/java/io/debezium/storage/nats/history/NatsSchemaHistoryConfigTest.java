/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats.history;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

/**
 * Unit tests for {@link NatsSchemaHistoryConfig} parsing. No NATS server is
 * required.
 *
 * @author Nick Chomey
 */
class NatsSchemaHistoryConfigTest {

    private static final String PREFIX = "schema.history.internal.";

    @Test
    public void shouldDefaultPublishRetrySettings() {
        Map<String, String> props = new HashMap<>();
        props.put(PREFIX + "nats.url", "nats://localhost:4222");
        NatsSchemaHistoryConfig config = new NatsSchemaHistoryConfig(Configuration.from(props));

        assertEquals(100L, config.getRetryDelayMs());
        assertEquals(20, config.getMaxRetries());
    }

    @Test
    public void shouldParsePublishRetrySettings() {
        Map<String, String> props = new HashMap<>();
        props.put(PREFIX + "nats.url", "nats://localhost:4222");
        props.put(PREFIX + "nats.retry.delay.ms", "250");
        props.put(PREFIX + "nats.max.retries", "5");
        NatsSchemaHistoryConfig config = new NatsSchemaHistoryConfig(Configuration.from(props));

        assertEquals(250L, config.getRetryDelayMs());
        assertEquals(5, config.getMaxRetries());
    }
}
