/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.enterprise.inject.Instance;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.engine.RecordChangeEvent;
import io.quarkus.debezium.configuration.PostgresDatasourceConfiguration;
import io.quarkus.debezium.engine.capture.CapturingInvokerRegistry;

class PostgresEngineProducerTest {

    private final Instance<PostgresDatasourceConfiguration> instance = Mockito.mock(Instance.class);
    private final CapturingInvokerRegistry<RecordChangeEvent<SourceRecord>> registry = identifier -> event -> {
    };
    private final PostgresEngineProducer underTest = new PostgresEngineProducer(registry, Mockito.mock(DefaultStateHandler.class), instance);

    @Test
    @DisplayName("should merge configurations when debezium configuration doesn't contain datasource information")
    void shouldMergeConfigurationsWhenDebeziumConfigurationIsWithoutDatasourceInformation() {
        List<PostgresDatasourceConfiguration> configurations = List.of(new PostgresDatasourceConfiguration(
                "host",
                "username",
                "password",
                "database",
                "1926",
                true,
                "<default>"));
        when(instance.iterator()).thenReturn(configurations.iterator());
        when(instance.stream()).thenReturn(configurations.stream());

        assertThat(underTest.engine(() -> new HashMap<>(Map.of("name", "test")))
                .configuration())
                .isEqualTo(Map.of(
                        "connector.class", "io.debezium.connector.postgresql.PostgresConnector",
                        "name", "default",
                        "database.hostname", "host",
                        "database.port", "1926",
                        "database.user", "username",
                        "database.password", "password",
                        "database.dbname", "database"));
    }

    @Test
    @DisplayName("should use debezium configurations when contains datasource information")
    void shouldUseDebeziumConfigurationWhenContainsDatasourceInformation() {
        List<PostgresDatasourceConfiguration> configurations = List.of(new PostgresDatasourceConfiguration(
                "host",
                "username",
                "password",
                "database",
                "1926",
                true,
                "<default>"));
        when(instance.iterator()).thenReturn(configurations.iterator());
        when(instance.stream()).thenReturn(configurations.stream());

        assertThat(underTest.engine(() -> new HashMap<>(Map.of("name", "test", "database.hostname", "native")))
                .configuration())
                .isEqualTo(Map.of(
                        "connector.class", "io.debezium.connector.postgresql.PostgresConnector",
                        "name", "test",
                        "database.hostname", "native"));
    }
}
