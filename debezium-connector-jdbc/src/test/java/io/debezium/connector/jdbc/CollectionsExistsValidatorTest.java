/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(defaultProperties());

        assertThat(validator(config, null, false).resolveCollectionNames(List.of("server.inventory.customers", "server.inventory.orders")))
                .containsExactly("server_inventory_customers", "server_inventory_orders");
    }

    @Test
    public void shouldResolveLiteralCollectionName() {
        final Map<String, String> properties = defaultProperties();
        properties.put(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "inventory.customers");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);

        assertThat(validator(config, null, true).resolveCollectionNames(Set.of()))
                .containsExactly("inventory.customers");
    }

    @Test
    public void shouldDeferTopicPlaceholderValidationWithTopicsRegex() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(defaultProperties());

        assertThatCode(() -> validator(config, null, true).validate()).doesNotThrowAnyException();
    }

    @Test
    public void shouldResolveAssignedTopicBasedCollectionNames() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(defaultProperties());

        assertThat(validator(config, null, true).resolveCollectionNames(Set.of("server.inventory.customers")))
                .containsExactly("server_inventory_customers");
    }

    @Test
    public void shouldRejectTopicPlaceholderWithoutTopics() {
        final Map<String, String> properties = defaultProperties();

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);

        assertThatThrownBy(() -> validator(config, null, false).validate())
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("requires 'topics' or 'topics.regex'");
    }

    @Test
    public void shouldRejectSourceFieldPlaceholders() {
        final Map<String, String> properties = defaultProperties();
        properties.put(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "${source.schema}_${source.table}");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);

        assertThatThrownBy(() -> validator(config, null, false).resolveCollectionNames(Set.of()))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("source field placeholders");
    }

    private static Map<String, String> defaultProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.NONE_VALIDATED.getValue());
        return properties;
    }

    private static CollectionsExistsValidator validator(JdbcSinkConnectorConfig config, String topics, boolean topicsRegexConfigured) {
        return new CollectionsExistsValidator(config, mock(SessionFactory.class), mock(DatabaseDialect.class), topics, topicsRegexConfigured);
    }
}
