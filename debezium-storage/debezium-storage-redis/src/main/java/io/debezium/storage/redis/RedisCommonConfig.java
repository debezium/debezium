/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.util.Collect;

public class RedisCommonConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisCommonConfig.class);

    public static final String CONFIGURATION_FIELD_PREFIX_STRING = "redis.";

    private static final Field PROP_ADDRESS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "address")
            .withDescription("The url that will be used to access Redis")
            .required();

    private static final Field PROP_USER = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "user")
            .withDescription("The user that will be used to access Redis");

    private static final Field PROP_PASSWORD = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "password")
            .withDescription("The password that will be used to access Redis");

    private static final boolean DEFAULT_SSL_ENABLED = false;
    private static final Field PROP_SSL_ENABLED = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "ssl.enabled")
            .withDescription("Use SSL for Redis connection")
            .withDefault(DEFAULT_SSL_ENABLED);

    private static final Integer DEFAULT_CONNECTION_TIMEOUT = 2000;
    private static final Field PROP_CONNECTION_TIMEOUT = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "connection.timeout.ms")
            .withDescription("Connection timeout (in ms)")
            .withDefault(DEFAULT_CONNECTION_TIMEOUT);

    private static final Integer DEFAULT_SOCKET_TIMEOUT = 2000;
    private static final Field PROP_SOCKET_TIMEOUT = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "socket.timeout.ms")
            .withDescription("Socket timeout (in ms)")
            .withDefault(DEFAULT_SOCKET_TIMEOUT);

    private static final Integer DEFAULT_RETRY_INITIAL_DELAY = 300;
    private static final Field PROP_RETRY_INITIAL_DELAY = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "retry.initial.delay.ms")
            .withDescription("Initial retry delay (in ms)")
            .withDefault(DEFAULT_RETRY_INITIAL_DELAY);

    private static final Integer DEFAULT_RETRY_MAX_DELAY = 10000;
    private static final Field PROP_RETRY_MAX_DELAY = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "retry.max.delay.ms")
            .withDescription("Maximum retry delay (in ms)")
            .withDefault(DEFAULT_RETRY_MAX_DELAY);

    private static final boolean DEFAULT_WAIT_ENABLED = false;
    private static final Field PROP_WAIT_ENABLED = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "wait.enabled")
            .withDescription(
                    "Enables wait for replica. In case Redis is configured with a replica shard, this allows to verify that the data has been written to the replica.")
            .withDefault(DEFAULT_WAIT_ENABLED);

    private static final long DEFAULT_WAIT_TIMEOUT = 1000L;
    private static final Field PROP_WAIT_TIMEOUT = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "wait.timeout.ms")
            .withDescription("Timeout when wait for replica")
            .withDefault(DEFAULT_WAIT_TIMEOUT);

    private static final boolean DEFAULT_WAIT_RETRY_ENABLED = false;
    private static final Field PROP_WAIT_RETRY_ENABLED = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "wait.retry.enabled")
            .withDescription("Enables retry on wait for replica failure")
            .withDefault(DEFAULT_WAIT_RETRY_ENABLED);

    private static final long DEFAULT_WAIT_RETRY_DELAY = 1000L;
    private static final Field PROP_WAIT_RETRY_DELAY = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "wait.retry.delay.ms")
            .withDescription("Delay of retry on wait for replica failure")
            .withDefault(DEFAULT_WAIT_RETRY_DELAY);

    private String address;
    private String user;
    private String password;
    private boolean sslEnabled;

    private Integer initialRetryDelay;
    private Integer maxRetryDelay;

    private Integer connectionTimeout;
    private Integer socketTimeout;

    private boolean waitEnabled;
    private long waitTimeout;
    private boolean waitRetryEnabled;
    private long waitRetryDelay;

    public RedisCommonConfig(Configuration config, String prefix) {
        config = config.subset(prefix, true);
        LOGGER.info("Configuration for '{}' with prefix '{}': {}", getClass().getSimpleName(), prefix, config.asMap());
        if (!config.validateAndRecord(getAllConfigurationFields(), error -> LOGGER.error("Validation error for property with prefix '{}': {}", prefix, error))) {
            throw new DebeziumException(
                    String.format("Error configuring an instance of '%s' with prefix '%s'; check the logs for errors", getClass().getSimpleName(), prefix));
        }
        init(config);
    }

    protected List<Field> getAllConfigurationFields() {
        return Collect.arrayListOf(PROP_ADDRESS, PROP_USER, PROP_PASSWORD, PROP_SSL_ENABLED, PROP_CONNECTION_TIMEOUT, PROP_SOCKET_TIMEOUT, PROP_RETRY_INITIAL_DELAY,
                PROP_RETRY_MAX_DELAY, PROP_WAIT_ENABLED, PROP_WAIT_TIMEOUT, PROP_WAIT_RETRY_ENABLED, PROP_WAIT_RETRY_DELAY);
    }

    protected void init(Configuration config) {
        address = config.getString(PROP_ADDRESS);
        user = config.getString(PROP_USER);
        password = config.getString(PROP_PASSWORD);
        sslEnabled = config.getBoolean(PROP_SSL_ENABLED);

        initialRetryDelay = config.getInteger(PROP_RETRY_INITIAL_DELAY);
        maxRetryDelay = config.getInteger(PROP_RETRY_MAX_DELAY);

        connectionTimeout = config.getInteger(PROP_CONNECTION_TIMEOUT);
        socketTimeout = config.getInteger(PROP_SOCKET_TIMEOUT);

        waitEnabled = config.getBoolean(PROP_WAIT_ENABLED);
        waitTimeout = config.getLong(PROP_WAIT_TIMEOUT);
        waitRetryEnabled = config.getBoolean(PROP_WAIT_RETRY_ENABLED);
        waitRetryDelay = config.getLong(PROP_WAIT_RETRY_DELAY);
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getAddress() {
        return address;
    }

    public String getUser() {
        return user;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public Integer getInitialRetryDelay() {
        return initialRetryDelay;
    }

    public Integer getMaxRetryDelay() {
        return maxRetryDelay;
    }

    public Integer getConnectionTimeout() {
        return connectionTimeout;
    }

    public Integer getSocketTimeout() {
        return socketTimeout;
    }

    public boolean isWaitEnabled() {
        return waitEnabled;
    }

    public long getWaitTimeout() {
        return waitTimeout;
    }

    public boolean isWaitRetryEnabled() {
        return waitRetryEnabled;
    }

    public long getWaitRetryDelay() {
        return waitRetryDelay;
    }

}
