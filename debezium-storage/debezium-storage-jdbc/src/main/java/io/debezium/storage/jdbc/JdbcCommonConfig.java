/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.util.Collect;

/**
 * Configuration options shared between offset and history storage modules.
 *
 * @author Jiri Pechanec
 *
 */
public class JdbcCommonConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcCommonConfig.class);

    public static final String CONFIGURATION_FIELD_PREFIX_STRING = "jdbc.";

    public static final Field PROP_JDBC_URL = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "url")
            .withDescription("URL of the database which will be used to access the database storage")
            .withValidation(Field::isRequired);

    public static final Field PROP_USER = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "user")
            .withDescription("Username of the database which will be used to access the database storage")
            .withValidation(Field::isRequired);

    public static final Field PROP_PASSWORD = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "password")
            .withDescription("Password of the database which will be used to access the database storage")
            .withValidation(Field::isRequired);

    private String jdbcUrl;
    private String user;
    private String password;

    public JdbcCommonConfig(Configuration config, String prefix) {
        config = config.subset(prefix, true);
        LOGGER.info("Configuration for '{}' with prefix '{}': {}", getClass().getSimpleName(), prefix, config.withMaskedPasswords().asMap());
        if (!config.validateAndRecord(getAllConfigurationFields(), error -> LOGGER.error("Validation error for property with prefix '{}': {}", prefix, error))) {
            throw new DebeziumException(
                    String.format("Error configuring an instance of '%s' with prefix '%s'; check the logs for errors", getClass().getSimpleName(), prefix));
        }
        init(config);
    }

    protected List<Field> getAllConfigurationFields() {
        return Collect.arrayListOf(PROP_JDBC_URL, PROP_USER, PROP_PASSWORD);
    }

    protected void init(Configuration config) {
        jdbcUrl = config.getString(PROP_JDBC_URL);
        user = config.getString(PROP_USER);
        password = config.getString(PROP_PASSWORD);
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
