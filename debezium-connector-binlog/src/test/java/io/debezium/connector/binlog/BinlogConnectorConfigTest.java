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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.binlog.jdbc.LowerCaseTableNames;
import io.debezium.doc.FixFor;
import io.debezium.relational.TableId;

/**
 * @author Randall Hauch
 */
public abstract class BinlogConnectorConfigTest<C extends SourceConnector> implements BinlogConnectorTest<C> {

    @Test
    void shouldReturnConfigurationDefinition() {
        assertConfigDefIsValid(getConnectorInstance(), getAllFields());
    }

    @ParameterizedTest
    @EnumSource(LowerCaseTableNames.class)
    @FixFor("debezium/dbz#125")
    void shouldMatchSignalCollectionsUsingServerIdentifierRules(LowerCaseTableNames setting) {
        final var config = getConnectorConfig(Configuration.create()
                .with(BinlogConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.SIGNAL_DATA_COLLECTION, "Inventory.Debezium_Signal,Other.Audit_Signal")
                .build());
        config.setLowerCaseTableNames(setting);

        assertThat(config.isSignalDataCollection(new TableId("Inventory", null, "Debezium_Signal"))).isTrue();
        assertThat(config.isSignalDataCollection(new TableId("inventory", null, "debezium_signal"))).isEqualTo(setting.isCaseInsensitive());
        assertThat(config.isSignalDataCollection(new TableId("INVENTORY", null, "DEBEZIUM_SIGNAL"))).isEqualTo(setting.isCaseInsensitive());
        assertThat(config.isSignalDataCollection(new TableId("Other", null, "Audit_Signal"))).isTrue();
        assertThat(config.isSignalDataCollection(new TableId("other", null, "audit_signal"))).isEqualTo(setting.isCaseInsensitive());
        assertThat(config.isSignalDataCollection(new TableId("Inventory", null, "products"))).isFalse();
        assertThat(config.isSignalDataCollection(new TableId("Other", null, "Debezium_Signal"))).isFalse();
        assertThat(config.isSignalDataCollection(null)).isFalse();
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
            assertThat(key.validator).isNull();
            assertThat(key.recommender).isNull();
        });
    }

    protected abstract C getConnectorInstance();

    protected abstract Field.Set getAllFields();

    protected abstract BinlogConnectorConfig getConnectorConfig(Configuration config);

}
