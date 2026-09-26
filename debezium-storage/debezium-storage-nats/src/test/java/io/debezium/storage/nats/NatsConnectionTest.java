/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

/**
 * Unit tests for {@link NatsConnection} that need no NATS server.
 *
 * @author Nick Chomey
 */
class NatsConnectionTest {

    @Test
    public void shouldDeriveDefaultProbeRetryBudgetFromReconnectWait() {
        Map<String, String> props = new HashMap<>();
        props.put("nats.url", "nats://localhost:4222");
        NatsConnection connection = new NatsConnection(new NatsCommonConfig(Configuration.from(props)));

        // Default reconnect wait of 2000ms yields 20 retries at 100ms intervals
        assertEquals(20, connection.getRetryBudget());
    }

    @Test
    public void shouldNotOverflowProbeRetryBudgetForVeryLargeReconnectWait() {
        Map<String, String> props = new HashMap<>();
        props.put("nats.url", "nats://localhost:4222");
        // A wait above Integer.MAX_VALUE milliseconds used to truncate to a
        // negative budget, which RetryingRunnable treats as infinite retries
        props.put("nats.reconnect.wait.ms", "4000000000");
        NatsConnection connection = new NatsConnection(new NatsCommonConfig(Configuration.from(props)));

        assertEquals(40_000_000, connection.getRetryBudget());
    }

    @Test
    public void shouldClampProbeRetryBudgetAtIntegerRange() {
        Map<String, String> props = new HashMap<>();
        props.put("nats.url", "nats://localhost:4222");
        props.put("nats.reconnect.wait.ms", String.valueOf(Long.MAX_VALUE));
        NatsConnection connection = new NatsConnection(new NatsCommonConfig(Configuration.from(props)));

        assertEquals(Integer.MAX_VALUE, connection.getRetryBudget());
    }
}
