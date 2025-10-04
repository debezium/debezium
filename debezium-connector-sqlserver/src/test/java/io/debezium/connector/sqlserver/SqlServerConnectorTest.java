/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Connector;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;

public class SqlServerConnectorTest {
    SqlServerConnector connector;

    @Before
    public void before() {
        connector = new SqlServerConnector();
    }

    @Test
    public void testValidateUnableToConnectNoThrow() {
        Map<String, String> config = new HashMap<>();
        config.put(CommonConnectorConfig.TOPIC_PREFIX.name(), "dbserver1");
        config.put(SqlServerConnectorConfig.HOSTNAME.name(), "narnia");
        config.put(SqlServerConnectorConfig.PORT.name(), "4321");
        config.put(SqlServerConnectorConfig.DATABASE_NAMES.name(), "sqlserver");
        config.put(SqlServerConnectorConfig.USER.name(), "pikachu");
        config.put(SqlServerConnectorConfig.PASSWORD.name(), "raichu");

        Config validated = connector.validate(config);
        ConfigValue hostName = getHostName(validated).orElseThrow(() -> new IllegalArgumentException("Host name config option not found"));
        assertThat(hostName.errorMessages().get(0).startsWith("Unable to connect:"));
    }

    private Optional<ConfigValue> getHostName(Config config) {
        return config.configValues()
                .stream()
                .filter(value -> value.name().equals(SqlServerConnectorConfig.HOSTNAME.name()))
                .findFirst();
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
            if (expected.equals(SqlServerConnectorConfig.SCHEMA_HISTORY) || expected.equals(CommonConnectorConfig.TOPIC_NAMING_STRATEGY)) {
                assertThat(((Class<?>) key.defaultValue).getName()).isEqualTo((String) expected.defaultValue());
            }
            else if (expected.type() == ConfigDef.Type.LIST && key.defaultValue != null) {
                assertThat(key.defaultValue).isEqualTo(Arrays.asList(expected.defaultValue()));
            }
            assertThat(key.dependents).isEqualTo(expected.dependents());
            assertThat(key.width).isNotNull();
            assertThat(key.group).isNotNull();
            assertThat(key.orderInGroup).isGreaterThan(0);
            if ((key.validator != null)) {
                assertThat(key.validator)
                        .withFailMessage("Validator should be instance of ConfigDef.Validator for field: %s", expected.name())
                        .isInstanceOf(ConfigDef.Validator.class);
            }
            else {
                assertThat(key.validator).isNull();
            }
            assertThat(key.recommender).isNull();
        });
    }
}
