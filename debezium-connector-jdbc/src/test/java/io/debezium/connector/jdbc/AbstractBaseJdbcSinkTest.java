/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.util.RandomTableNameGenerator;

/**
 * Abstract Base JDBC Sink Connector test.
 *
 * @author rk3rn3r
 */
public abstract class AbstractBaseJdbcSinkTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBaseJdbcSinkTest.class);

    private final RandomTableNameGenerator randomTableNameGenerator = new RandomTableNameGenerator();

    /**
     * Returns the base sink connector configuration to talk to the database instance
     * that was started by the TestContainers framework.
     */
    protected static Map<String, String> baseSinkConfig() {
        final Map<String, String> config = new LinkedHashMap<>();
        // Create base configuration from system properties
        config.put(JdbcSinkConnectorConfig.ENABLE_SHARED_CHANGE_EVENT_SINK_FIELD.name(),
                System.getProperty("enable.sces", "false"));
        return config;
    }

    protected static Map<String, String> mergeWithBaseConfig(Map<String, String> properties) {
        final Map<String, String> config = baseSinkConfig();
        config.putAll(properties);
        return config;
    }

    protected static @NonNull JdbcSinkConnectorConfig getConfig(Map<String, String> properties) {
        return new JdbcSinkConnectorConfig(mergeWithBaseConfig(properties));
    }

    /**
     * Returns a constructed topic name based on the prefix, schema, and table names.
     */
    protected String topicName(String prefix, String schemaName, String tableName) {
        return prefix + "." + schemaName + "." + tableName;
    }

    /**
     * Returns a random table name that can be used by the test.
     */
    protected String randomTableName() {
        return randomTableNameGenerator.randomName();
    }

    protected void assertExceptionCauseMessage(Throwable t, String messagePattern) {
        Throwable cause = t;
        while (cause != null) {
            if (cause.getMessage() != null && cause.getMessage().matches(messagePattern)) {
                return;
            }
            cause = cause.getCause();
        }
        assertThat(cause).as("No exception in cause chain matches pattern: " + messagePattern).isNotNull();
    }

    protected void assertExceptionCausedBy(Throwable t, Class<? extends Throwable> causedBy, String messagePattern) {
        Throwable cause = t;
        while (cause != null) {
            if (cause.getMessage() != null && cause.getMessage().matches(messagePattern) && cause.getClass() == causedBy) {
                return;
            }
            cause = cause.getCause();
        }
        assertThat(cause).as("No exception in cause chain matches pattern: " + messagePattern).isNotNull();
    }

    protected void assertNoExceptionCauseMessage(Throwable t, String substring) {
        Throwable cause = t;
        while (cause != null) {
            assertThat(cause.getMessage()).as("Exception cause should not contain: " + substring)
                    .doesNotContain(substring);
            cause = cause.getCause();
        }
    }

}
