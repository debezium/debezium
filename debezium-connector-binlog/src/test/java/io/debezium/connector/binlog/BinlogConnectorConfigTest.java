/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.source.SourceConnector;
import org.junit.Test;

import io.debezium.config.Field;

/**
 * @author Randall Hauch
 */
public abstract class BinlogConnectorConfigTest<C extends SourceConnector> implements BinlogConnectorTest<C> {

    @Test
    public void shouldReturnConfigurationDefinition() {
        assertConfigDefIsValid(getConnectorInstance(), getAllFields());
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
            else if (expected.type() == Type.LIST && key.defaultValue != null) {
                assertThat(key.defaultValue).isEqualTo(Arrays.asList(expected.defaultValue()));
            }
            else if (!expected.equals(BinlogConnectorConfig.SERVER_ID)) {
                assertThat(key.defaultValue).isEqualTo(expected.defaultValue());
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

    protected abstract C getConnectorInstance();

    protected abstract Field.Set getAllFields();

}
