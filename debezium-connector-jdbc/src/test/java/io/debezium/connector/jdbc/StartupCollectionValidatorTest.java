/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;

@Tag("UnitTests")
public class StartupCollectionValidatorTest {

    @Test
    public void shouldExposeSchemaEvolutionModeCapabilities() {
        final SchemaEvolutionMode mode = SchemaEvolutionMode.parse("none-validated");

        assertThat(mode).isEqualTo(SchemaEvolutionMode.NONE_VALIDATED);
        assertThat(SchemaEvolutionMode.NONE.validateOnStartup()).isFalse();
        assertThat(SchemaEvolutionMode.NONE.schemaEvolution()).isFalse();
        assertThat(mode.validateOnStartup()).isTrue();
        assertThat(mode.schemaEvolution()).isFalse();
        assertThat(SchemaEvolutionMode.BASIC.validateOnStartup()).isFalse();
        assertThat(SchemaEvolutionMode.BASIC.schemaEvolution()).isTrue();
    }

    @Test
    public void shouldResolveTopicBasedCollectionNames() {
        final Map<String, String> properties = defaultProperties();
        properties.put("topics", "server.inventory.customers, server.inventory.orders");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);

        assertThat(StartupCollectionValidator.resolveCollectionNames(config, properties))
                .containsExactly("server_inventory_customers", "server_inventory_orders");
    }

    @Test
    public void shouldResolveLiteralCollectionName() {
        final Map<String, String> properties = defaultProperties();
        properties.put("topics.regex", "server\\.inventory\\..*");
        properties.put(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "inventory.customers");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);

        assertThat(StartupCollectionValidator.resolveCollectionNames(config, properties))
                .containsExactly("inventory.customers");
    }

    @Test
    public void shouldRejectTopicPlaceholderWithTopicsRegex() {
        final Map<String, String> properties = defaultProperties();
        properties.put("topics.regex", "server\\.inventory\\..*");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);

        assertThatThrownBy(() -> StartupCollectionValidator.resolveCollectionNames(config, properties))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("topics.regex");
    }

    @Test
    public void shouldRejectTopicPlaceholderWithoutTopics() {
        final Map<String, String> properties = defaultProperties();

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);

        assertThatThrownBy(() -> StartupCollectionValidator.resolveCollectionNames(config, properties))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("requires statically configured 'topics'");
    }

    @Test
    public void shouldRejectSourceFieldPlaceholders() {
        final Map<String, String> properties = defaultProperties();
        properties.put(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "${source.schema}_${source.table}");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);

        assertThatThrownBy(() -> StartupCollectionValidator.resolveCollectionNames(config, properties))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("source field placeholders");
    }

    private static Map<String, String> defaultProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.NONE_VALIDATED.getValue());
        return properties;
    }
}
