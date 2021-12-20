/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.ConfigDefinition;
import io.debezium.config.Field;

public class JdbcSinkConnectorConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkConnectorTask.class);

    public static final String CONNECTION_URL = "connection.url";
    public static final String CONNECTION_USER = "connection.username";
    public static final String CONNECTION_PASSWORD = "connection.password";
    public static final String CONNECTION_POOL_MIN_SIZE = "connection.pool.min_size";
    public static final String CONNECTION_POOL_MAX_SIZE = "connection.pool.max_size";
    public static final String CONNECTION_POOL_ACQUIRE_INCREMENT = "connection.pool.acquire_increment";
    public static final String CONNECTION_POOL_TIMEOUT = "connection.pool.timeout";

    private final io.debezium.config.Configuration config;

    public JdbcSinkConnectorConfig(Map<String, String> props) {
        config = io.debezium.config.Configuration.from(props);
    }

    public void validate() {
        // Validation
        if (!config.validateAndRecord(JdbcSinkConnectorConfig.ALL_FIELDS, LOGGER::error)) {
            throw new ConnectException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Starting {} with configuration:", getClass().getSimpleName());
            config.withMaskedPasswords().forEach((propName, propValue) -> {
                LOGGER.info("   {} = {}", propName, propValue);
            });
        }
    }

    public static final Field CONNECTION_URL_FIELD = Field.create(CONNECTION_URL)
            .withDisplayName("Hostname")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("Valid JDBC URL");

    public static final Field CONNECTION_USER_FIELD = Field.create(CONNECTION_USER)
            .withDisplayName("User")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 2))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("Name of the database user to be used when connecting to the connection.");

    public static final Field CONNECTION_PASSWORD_FIELD = Field.create(CONNECTION_PASSWORD)
            .withDisplayName("Password")
            .withType(ConfigDef.Type.PASSWORD)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 3))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("Password of the database user to be used when connecting to the connection.");

    public static final Field CONNECTION_POOL_MIN_SIZE_FIELD = Field.create(CONNECTION_POOL_MIN_SIZE)
            .withDisplayName("Connection pool minimum size")
            .withType(ConfigDef.Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 4))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(5)
            .withDescription("Minimum number of connection in the connection pool");

    public static final Field CONNECTION_POOL_MAX_SIZE_FIELD = Field.create(CONNECTION_POOL_MAX_SIZE)
            .withDisplayName("Connection pool maximum size")
            .withType(ConfigDef.Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 5))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(32)
            .withDescription("Maximum number of connection in the connection pool");

    public static final Field CONNECTION_POOL_ACQUIRE_INCREMENT_FIELD = Field.create(CONNECTION_POOL_ACQUIRE_INCREMENT)
            .withDisplayName("Connection pool acquire increment")
            .withType(ConfigDef.Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 6))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(32)
            .withDescription("Connection pool acquire increment");

    public static final Field CONNECTION_POOL_TIMEOUT_FIELD = Field.create(CONNECTION_POOL_TIMEOUT)
            .withDisplayName("Connection pool timeout")
            .withType(ConfigDef.Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 7))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(1800)
            .withDescription("CConnection pool timeout");

    protected static final ConfigDefinition CONFIG_DEFINITION = ConfigDefinition.editor()
            .connector(
                    CONNECTION_URL_FIELD,
                    CONNECTION_USER_FIELD,
                    CONNECTION_PASSWORD_FIELD,
                    CONNECTION_POOL_MIN_SIZE_FIELD,
                    CONNECTION_POOL_MAX_SIZE_FIELD,
                    CONNECTION_POOL_ACQUIRE_INCREMENT_FIELD,
                    CONNECTION_POOL_TIMEOUT_FIELD)
            .create();

    /** makes {@link org.hibernate.cfg.Configuration} from connector config
     *
     * @return {@link org.hibernate.cfg.Configuration}
     */
    public org.hibernate.cfg.Configuration getHibernateConfiguration() {
        org.hibernate.cfg.Configuration hibernateConfig = new org.hibernate.cfg.Configuration();
        hibernateConfig.setProperty("hibernate.connection.url", config.getString(CONNECTION_URL_FIELD));
        hibernateConfig.setProperty("hibernate.connection.username", config.getString(CONNECTION_USER_FIELD));
        hibernateConfig.setProperty("hibernate.connection.password", config.getString(CONNECTION_PASSWORD_FIELD));
        hibernateConfig.setProperty("hibernate.c3p0.min_size", config.getString(CONNECTION_POOL_MIN_SIZE_FIELD));
        hibernateConfig.setProperty("hibernate.c3p0.max_size", config.getString(CONNECTION_POOL_MAX_SIZE_FIELD));
        hibernateConfig.setProperty("hibernate.c3p0.acquire_increment", config.getString(CONNECTION_POOL_ACQUIRE_INCREMENT_FIELD));
        hibernateConfig.setProperty("hibernate.c3p0.timeout", config.getString(CONNECTION_POOL_TIMEOUT_FIELD));

        hibernateConfig.setProperty("hibernate.connection.provider_class", "org.hibernate.connection.C3P0ConnectionProvider");
        hibernateConfig.setProperty("hibernate.globally_quoted_identifiers", "true");
        hibernateConfig.setProperty("hibernate.hbm2ddl.auto", "update");
        hibernateConfig.setProperty("hibernate.show_sql", "true");

        return hibernateConfig;
    }

    protected static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public String getContextName() {
        return Module.contextName();
    }

    public String getConnectorName() {
        return Module.name();
    }
}
