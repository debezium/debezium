/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.util.Collect;

/**
 * Common configuration for NATS-based storage implementations.
 * Follows the same prefix-chaining pattern used by Redis storage configs:
 * a subclass passes a prefix (e.g. "offset.storage." or
 * {@code SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING}), and all NATS
 * properties are declared with the fixed prefix {@code "nats."}.
 *
 * Therefore, a field declared as {@code nats.url} becomes
 * {@code offset.storage.nats.url} or {@code schema.history.internal.nats.url}
 * depending on the caller-provided prefix.
 *
 * @author Nick Babcock
 */
public class NatsCommonConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(NatsCommonConfig.class);

    public static final String CONFIGURATION_FIELD_PREFIX_STRING = "nats.";

    public static final Field NATS_URL = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "url")
            .withDescription("The NATS server URL to connect to")
            .withDefault("nats://localhost:4222");

    public static final Field NATS_CONNECTION_TIMEOUT_MS = Field
            .create(CONFIGURATION_FIELD_PREFIX_STRING + "connection.timeout.ms")
            .withDescription("The timeout for establishing NATS connection in milliseconds")
            .withDefault(5000L);

    public static final Field NATS_MAX_RECONNECTS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "max.reconnects")
            .withDescription("Maximum number of reconnection attempts (-1 for unlimited)")
            .withDefault(-1);

    public static final Field NATS_RECONNECT_WAIT_MS = Field
            .create(CONFIGURATION_FIELD_PREFIX_STRING + "reconnect.wait.ms")
            .withDescription("Time to wait between reconnection attempts in milliseconds")
            .withDefault(2000L);

    private String natsUrl;
    private long connectionTimeoutMs;
    private int maxReconnects;
    private long reconnectWaitMs;

    protected final Configuration config; // subset view after prefix

    /**
     * Backward-compatible constructor without a prefix. Prefer using the
     * constructor that takes a prefix so that keys are properly chained.
     */
    public NatsCommonConfig(Configuration config) {
        this(config, "");
    }

    public NatsCommonConfig(Configuration config, String prefix) {
        Configuration subset = prefix == null || prefix.isEmpty() ? config : config.subset(prefix, true);
        this.config = subset;

        LOGGER.info("Configuration for '{}' with prefix '{}': {}", getClass().getSimpleName(), prefix,
                subset.withMaskedPasswords());
        if (!subset.validateAndRecord(getAllConfigurationFields(),
                error -> LOGGER.error("Validation error for property with prefix '{}': {}", prefix, error))) {
            throw new DebeziumException(String.format(
                    "Error configuring an instance of '%s' with prefix '%s'; check the logs for errors",
                    getClass().getSimpleName(), prefix));
        }
        init(subset);
    }

    protected List<Field> getAllConfigurationFields() {
        return Collect.arrayListOf(
                NATS_URL,
                NATS_CONNECTION_TIMEOUT_MS,
                NATS_MAX_RECONNECTS,
                NATS_RECONNECT_WAIT_MS);
    }

    protected void init(Configuration c) {
        this.natsUrl = c.getString(NATS_URL);
        this.connectionTimeoutMs = c.getLong(NATS_CONNECTION_TIMEOUT_MS);
        this.maxReconnects = c.getInteger(NATS_MAX_RECONNECTS);
        this.reconnectWaitMs = c.getLong(NATS_RECONNECT_WAIT_MS);
    }

    public String getNatsUrl() {
        return natsUrl;
    }

    public Duration getConnectionTimeout() {
        return Duration.ofMillis(connectionTimeoutMs);
    }

    public int getMaxReconnects() {
        return maxReconnects;
    }

    public Duration getReconnectWait() {
        return Duration.ofMillis(reconnectWaitMs);
    }

}
