/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Connector;
import org.junit.Before;
import org.junit.Test;

public class OracleConnectorTest {
    OracleConnector connector;

    @Before
    public void before() {
        connector = new OracleConnector();
    }

    @Test
    public void testValidateUnableToConnectNoThrow() {
        Map<String, String> config = new HashMap<>();
        config.put(OracleConnectorConfig.HOSTNAME.name(), "narnia");
        config.put(OracleConnectorConfig.PORT.name(), "4321");
        config.put(OracleConnectorConfig.DATABASE_NAME.name(), "oracle");
        config.put(OracleConnectorConfig.USER.name(), "pikachu");
        config.put(OracleConnectorConfig.PASSWORD.name(), "raichu");

        Config validated = connector.validate(config);
        for (ConfigValue value : validated.configValues()) {
            if (value.name().equals(OracleConnectorConfig.HOSTNAME.name())) {
                assertThat(value.errorMessages().get(0).startsWith("Unable to connect"));
            }
        }
    }

    @Test
    public void shouldReturnConfigurationDefinition() {
        assertConfigDefIsValid(connector, OracleConnectorConfig.ALL_FIELDS);
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
            if (expected.equals(OracleConnectorConfig.DATABASE_HISTORY)) {
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
