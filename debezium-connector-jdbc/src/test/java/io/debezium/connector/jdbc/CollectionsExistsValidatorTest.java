/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import org.hibernate.SessionFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;

@Tag("UnitTests")
public class CollectionsExistsValidatorTest {

    @Test
    public void shouldExposeSchemaEvolutionModeCapabilities() {
        final SchemaEvolutionMode mode = SchemaEvolutionMode.parse("none-validated");

        assertThat(mode).isEqualTo(SchemaEvolutionMode.NONE_VALIDATED);
        assertThat(SchemaEvolutionMode.NONE.validateOnStartup()).isFalse();
        assertThat(SchemaEvolutionMode.NONE.isSchemaEvolutionEnabled()).isFalse();
        assertThat(mode.validateOnStartup()).isTrue();
        assertThat(mode.isSchemaEvolutionEnabled()).isFalse();
        assertThat(SchemaEvolutionMode.BASIC.validateOnStartup()).isFalse();
        assertThat(SchemaEvolutionMode.BASIC.isSchemaEvolutionEnabled()).isTrue();
    }

    @Test
    public void shouldResolveTopicBasedCollectionNames() {
        final Map<String, String> properties = defaultProperties();
        properties.put("topics", "server.inventory.customers, server.inventory.orders");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);

        assertThat(validator(config, properties).resolveCollectionNames())
                .containsExactly("server_inventory_customers", "server_inventory_orders");
    }

    @Test
    public void shouldResolveLiteralCollectionName() {
        final Map<String, String> properties = defaultProperties();
        properties.put("topics.regex", "server\\.inventory\\..*");
        properties.put(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "inventory.customers");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);

        assertThat(validator(config, properties).resolveCollectionNames())
                .containsExactly("inventory.customers");
    }

    @Test
    public void shouldRejectTopicPlaceholderWithTopicsRegex() {
        final Map<String, String> properties = defaultProperties();
        properties.put("topics.regex", "server\\.inventory\\..*");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);

        assertThatThrownBy(() -> validator(config, properties).resolveCollectionNames())
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("topics.regex");
    }

    @Test
    public void shouldRejectTopicPlaceholderWithoutTopics() {
        final Map<String, String> properties = defaultProperties();

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);

        assertThatThrownBy(() -> validator(config, properties).resolveCollectionNames())
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("requires statically configured 'topics'");
    }

    @Test
    public void shouldRejectSourceFieldPlaceholders() {
        final Map<String, String> properties = defaultProperties();
        properties.put(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "${source.schema}_${source.table}");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);

        assertThatThrownBy(() -> validator(config, properties).resolveCollectionNames())
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("source field placeholders");
    }

    private static Map<String, String> defaultProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.NONE_VALIDATED.getValue());
        return properties;
    }

    private static CollectionsExistsValidator validator(JdbcSinkConnectorConfig config, Map<String, String> properties) {
        return new CollectionsExistsValidator(config, mock(SessionFactory.class), mock(DatabaseDialect.class), properties);
    }
}
