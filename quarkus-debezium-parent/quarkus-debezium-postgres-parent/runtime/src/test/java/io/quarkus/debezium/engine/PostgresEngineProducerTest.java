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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.runtime.CaptureGroup;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.quarkus.debezium.configuration.PostgresDatasourceConfiguration;
import io.quarkus.debezium.notification.QuarkusNotificationChannel;

class PostgresEngineProducerTest {

    private final Instance<PostgresDatasourceConfiguration> instance = Mockito.mock(Instance.class);
    private final QuarkusNotificationChannel quarkusNotificationChannel = Mockito.mock(QuarkusNotificationChannel.class);
    private final PostgresEngineProducer underTest = new PostgresEngineProducer(
            Mockito.mock(DefaultStateHandler.class), instance, quarkusNotificationChannel, event -> {

            });

    @BeforeEach
    void setUp() {
        when(quarkusNotificationChannel.name()).thenReturn("a_name");
    }

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

        assertThat(underTest.engine(new DebeziumEngineConfiguration() {
            @Override
            public Map<String, String> defaultConfiguration() {
                return new HashMap<>(Map.of("name", "test"));
            }

            @Override
            public Map<String, Capturing> capturing() {
                return Map.of();
            }
        }).get(new CaptureGroup("default"))
                .configuration())
                .isEqualTo(Map.of(
                        "connector.class", "io.debezium.connector.postgresql.PostgresConnector",
                        "name", "default",
                        "database.hostname", "host",
                        "database.port", "1926",
                        "database.user", "username",
                        "database.password", "password",
                        "notification.enabled.channels", "a_name",
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

        assertThat(underTest.engine(new DebeziumEngineConfiguration() {
            @Override
            public Map<String, String> defaultConfiguration() {
                return new HashMap<>(Map.of("name", "test", "database.hostname", "native"));
            }

            @Override
            public Map<String, Capturing> capturing() {
                return Map.of();
            }
        }).get(new CaptureGroup("default"))
                .configuration())
                .isEqualTo(Map.of(
                        "connector.class", "io.debezium.connector.postgresql.PostgresConnector",
                        "name", "test",
                        "notification.enabled.channels", "a_name",
                        "database.hostname", "native"));
    }
}
