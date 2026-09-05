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
import io.debezium.util.Strings;

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

    public static final Field NATS_USER = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "user")
            .withDescription("The username used to authenticate with the NATS server")
            .withDefault("");

    public static final Field NATS_PASSWORD = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "password")
            .withDescription("The password used to authenticate with the NATS server")
            .withDefault("");

    public static final Field NATS_TOKEN = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "token")
            .withDescription("The token used to authenticate with the NATS server")
            .withDefault("");

    public static final Field NATS_TLS_ENABLED = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "tls.enabled")
            .withDescription("Whether to use TLS when connecting to the NATS server")
            .withDefault(false);

    public static final Field NATS_TLS_TRUSTSTORE_PATH = Field
            .create(CONFIGURATION_FIELD_PREFIX_STRING + "tls.truststore.path")
            .withDescription("The path to the trust store file used for TLS connections to the NATS server")
            .withDefault("");

    public static final Field NATS_TLS_TRUSTSTORE_PASSWORD = Field
            .create(CONFIGURATION_FIELD_PREFIX_STRING + "tls.truststore.password")
            .withDescription("The password for the trust store file used for TLS connections to the NATS server")
            .withDefault("");

    public static final Field NATS_TLS_TRUSTSTORE_TYPE = Field
            .create(CONFIGURATION_FIELD_PREFIX_STRING + "tls.truststore.type")
            .withDescription("The type of the trust store file used for TLS connections to the NATS server")
            .withDefault("JKS");

    public static final Field NATS_TLS_KEYSTORE_PATH = Field
            .create(CONFIGURATION_FIELD_PREFIX_STRING + "tls.keystore.path")
            .withDescription("The path to the key store file used for TLS connections to the NATS server")
            .withDefault("");

    public static final Field NATS_TLS_KEYSTORE_PASSWORD = Field
            .create(CONFIGURATION_FIELD_PREFIX_STRING + "tls.keystore.password")
            .withDescription("The password for the key store file used for TLS connections to the NATS server")
            .withDefault("");

    public static final Field NATS_TLS_KEYSTORE_TYPE = Field
            .create(CONFIGURATION_FIELD_PREFIX_STRING + "tls.keystore.type")
            .withDescription("The type of the key store file used for TLS connections to the NATS server")
            .withDefault("JKS");

    private String natsUrl;
    private long connectionTimeoutMs;
    private int maxReconnects;
    private long reconnectWaitMs;
    private String user;
    private String password;
    private String token;
    private boolean tlsEnabled;
    private String tlsTruststorePath;
    private String tlsTruststorePassword;
    private String tlsTruststoreType;
    private String tlsKeystorePath;
    private String tlsKeystorePassword;
    private String tlsKeystoreType;

    protected final Configuration config; // subset view after prefix

    /**
     * Backward-compatible constructor without a prefix. Prefer using the
     * constructor that takes a prefix so that keys are properly chained.
     */
    public NatsCommonConfig(Configuration config) {
        this(config, "");
    }

    public NatsCommonConfig(Configuration config, String prefix) {
        Configuration subset = Strings.isNullOrBlank(prefix) ? config : config.subset(prefix, true);
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
                NATS_RECONNECT_WAIT_MS,
                NATS_USER,
                NATS_PASSWORD,
                NATS_TOKEN,
                NATS_TLS_ENABLED,
                NATS_TLS_TRUSTSTORE_PATH,
                NATS_TLS_TRUSTSTORE_PASSWORD,
                NATS_TLS_TRUSTSTORE_TYPE,
                NATS_TLS_KEYSTORE_PATH,
                NATS_TLS_KEYSTORE_PASSWORD,
                NATS_TLS_KEYSTORE_TYPE);
    }

    protected void init(Configuration c) {
        this.natsUrl = c.getString(NATS_URL);
        this.connectionTimeoutMs = c.getLong(NATS_CONNECTION_TIMEOUT_MS);
        this.maxReconnects = c.getInteger(NATS_MAX_RECONNECTS);
        this.reconnectWaitMs = c.getLong(NATS_RECONNECT_WAIT_MS);
        this.user = c.getString(NATS_USER);
        this.password = c.getString(NATS_PASSWORD);
        this.token = c.getString(NATS_TOKEN);
        this.tlsEnabled = c.getBoolean(NATS_TLS_ENABLED);
        this.tlsTruststorePath = c.getString(NATS_TLS_TRUSTSTORE_PATH);
        this.tlsTruststorePassword = c.getString(NATS_TLS_TRUSTSTORE_PASSWORD);
        this.tlsTruststoreType = c.getString(NATS_TLS_TRUSTSTORE_TYPE);
        this.tlsKeystorePath = c.getString(NATS_TLS_KEYSTORE_PATH);
        this.tlsKeystorePassword = c.getString(NATS_TLS_KEYSTORE_PASSWORD);
        this.tlsKeystoreType = c.getString(NATS_TLS_KEYSTORE_TYPE);
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

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getToken() {
        return token;
    }

    public boolean isTlsEnabled() {
        return tlsEnabled;
    }

    public String getTlsTruststorePath() {
        return tlsTruststorePath;
    }

    public String getTlsTruststorePassword() {
        return tlsTruststorePassword;
    }

    public String getTlsTruststoreType() {
        return tlsTruststoreType;
    }

    public String getTlsKeystorePath() {
        return tlsKeystorePath;
    }

    public String getTlsKeystorePassword() {
        return tlsKeystorePassword;
    }

    public String getTlsKeystoreType() {
        return tlsKeystoreType;
    }

}
