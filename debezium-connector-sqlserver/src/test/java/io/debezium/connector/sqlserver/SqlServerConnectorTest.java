/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Connector;
import org.junit.Before;
import org.junit.Test;

public class SqlServerConnectorTest {
    SqlServerConnector connector;

    @Before
    public void before() {
        connector = new SqlServerConnector();
    }

    @Test
    public void testValidateUnableToConnectNoThrow() {
        Map<String, String> config = new HashMap<>();
        config.put(SqlServerConnectorConfig.HOSTNAME.name(), "narnia");
        config.put(SqlServerConnectorConfig.PORT.name(), "4321");
        config.put(SqlServerConnectorConfig.DATABASE_NAME.name(), "sqlserver");
        config.put(SqlServerConnectorConfig.DATABASE_NAMES.name(), "sqlserver");
        config.put(SqlServerConnectorConfig.USER.name(), "pikachu");
        config.put(SqlServerConnectorConfig.PASSWORD.name(), "raichu");

        Config validated = connector.validate(config);
        for (ConfigValue value : validated.configValues()) {
            if (value.name().equals(SqlServerConnectorConfig.HOSTNAME.name())) {
                assertThat(value.errorMessages().get(0).startsWith("Unable to connect:"));
            }
        }
    }

    @Test
    public void shouldReturnSingleTaskConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(SqlServerConnectorConfig.HOSTNAME.name(), "narnia");
        config.put(SqlServerConnectorConfig.PORT.name(), "4321");
        config.put(SqlServerConnectorConfig.DATABASE_NAME.name(), "database1");
        config.put(SqlServerConnectorConfig.DATABASE_NAMES.name(), "database1,database2");
        config.put(SqlServerConnectorConfig.USER.name(), "pikachu");
        config.put(SqlServerConnectorConfig.PASSWORD.name(), "raichu");

        Config validated = connector.validate(config);
        connector.start(config);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertThat(taskConfigs.size()).isEqualTo(1);
        assertThat(taskConfigs.get(0).get(SqlServerConnectorConfig.DATABASE_NAME.name())).isEqualTo("database1");
        assertThat(taskConfigs.get(0).get(SqlServerConnectorConfig.DATABASE_NAMES.name())).isEqualTo("database1,database2");
    }

    @Test
    public void shouldReturnTwoTaskConfigs() {
        Map<String, String> config = new HashMap<>();
        config.put(SqlServerConnectorConfig.HOSTNAME.name(), "narnia");
        config.put(SqlServerConnectorConfig.PORT.name(), "4321");
        config.put(SqlServerConnectorConfig.DATABASE_NAME.name(), "database1");
        config.put(SqlServerConnectorConfig.DATABASE_NAMES.name(), "database1,database2");
        config.put(SqlServerConnectorConfig.USER.name(), "pikachu");
        config.put(SqlServerConnectorConfig.PASSWORD.name(), "raichu");

        Config validated = connector.validate(config);
        connector.start(config);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(2);
        assertThat(taskConfigs.size()).isEqualTo(2);
        assertThat(taskConfigs.get(0).get(SqlServerConnectorConfig.DATABASE_NAME.name())).isEqualTo("database1");
        assertThat(taskConfigs.get(0).get(SqlServerConnectorConfig.DATABASE_NAMES.name())).isEqualTo("database1");
        assertThat(taskConfigs.get(1).get(SqlServerConnectorConfig.DATABASE_NAME.name())).isEqualTo("database2");
        assertThat(taskConfigs.get(1).get(SqlServerConnectorConfig.DATABASE_NAMES.name())).isEqualTo("database2");
    }

    @Test
    public void shouldThrowInvalidArgumentException() {
        Map<String, String> config = new HashMap<>();
        config.put(SqlServerConnectorConfig.HOSTNAME.name(), "narnia");
        config.put(SqlServerConnectorConfig.PORT.name(), "4321");
        config.put(SqlServerConnectorConfig.DATABASE_NAME.name(), "database1");
        config.put(SqlServerConnectorConfig.USER.name(), "pikachu");
        config.put(SqlServerConnectorConfig.PASSWORD.name(), "raichu");
        config.put(SqlServerConnectorConfig.DATABASE_NAMES.name(), ",");

        Config validated = connector.validate(config);
        connector.start(config);

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            connector.taskConfigs(1);
        });
    }

    @Test
    public void shouldReturnConfigurationDefinition() {
        assertConfigDefIsValid(connector, SqlServerConnectorConfig.ALL_FIELDS);
    }

    protected static void assertConfigDefIsValid(Connector connector, io.debezium.config.Field.Set fields) {
        ConfigDef configDef = connector.config();
        assertThat(configDef).isNotNull();
        fields.forEach(expected -> {
            assertThat(configDef.names()).contains(expected.name());
            ConfigKey key = configDef.configKeys().get(expected.name());
            assertThat(key).isNotNull();
            assertThat(key.name).isEqualTo(expected.name());
            assertThat(key.displayName).isEqualTo(expected.displayName());
            assertThat(key.importance).isEqualTo(expected.importance());
            assertThat(key.documentation).isEqualTo(expected.description());
            assertThat(key.type).isEqualTo(expected.type());
            if (expected.equals(SqlServerConnectorConfig.DATABASE_HISTORY)) {
                assertThat(((Class<?>) key.defaultValue).getName()).isEqualTo((String) expected.defaultValue());
            }
            assertThat(key.dependents).isEqualTo(expected.dependents());
            assertThat(key.width).isNotNull();
            assertThat(key.group).isNotNull();
            assertThat(key.orderInGroup).isGreaterThan(0);
            assertThat(key.validator).isNull();
            assertThat(key.recommender).isNull();
        });
    }
}
