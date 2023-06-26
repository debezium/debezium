/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.Connector;
import org.junit.Test;

/**
 * @author Randall Hauch
 */
public class MySqlConnectorTest {

    @Test
    public void shouldReturnConfigurationDefinition() {
        assertConfigDefIsValid(new MySqlConnector(), MySqlConnectorConfig.ALL_FIELDS);
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
            if (expected.type() == Type.CLASS) {
                assertThat(((Class<?>) key.defaultValue).getName()).isEqualTo((String) expected.defaultValue());
            }
            else if (expected.type() == ConfigDef.Type.LIST && key.defaultValue != null) {
                assertThat(key.defaultValue).isEqualTo(Arrays.asList(expected.defaultValue()));
            }
            else if (!expected.equals(MySqlConnectorConfig.SERVER_ID)) {
                assertThat(key.defaultValue).isEqualTo(expected.defaultValue());
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
