/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.runtime.CaptureGroup;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.quarkus.debezium.configuration.PostgresDatasourceConfiguration;
import io.quarkus.debezium.notification.QuarkusNotificationChannel;

class PostgresEngineProducerTest {

    private final QuarkusNotificationChannel quarkusNotificationChannel = mock(QuarkusNotificationChannel.class);

    @Test
    @DisplayName("should merge configurations when debezium configuration doesn't contain datasource information")
    void shouldMergeConfigurationsWhenDebeziumConfigurationIsWithoutDatasourceInformation() {
        when(quarkusNotificationChannel.name()).thenReturn("a_name");

        var underTest = new PostgresEngineProducer(mock(DefaultStateHandler.class),
                Map.of(
                        "default",
                        new PostgresDatasourceConfiguration(
                                "host",
                                "username",
                                "password",
                                "database",
                                "1926",
                                true,
                                "<default>")),
                quarkusNotificationChannel,
                event -> {
                    /* ignore */ });

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
        when(quarkusNotificationChannel.name()).thenReturn("a_name");

        var underTest = new PostgresEngineProducer(mock(DefaultStateHandler.class),
                Map.of(
                        "default", new PostgresDatasourceConfiguration(
                                "host",
                                "username",
                                "password",
                                "database",
                                "1926",
                                true,
                                "<default>")),
                quarkusNotificationChannel,
                event -> {
                    /* ignore */ });

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

    @Test
    @DisplayName("should create multiple configurations based on debezium and quarkus datasource")
    void shouldCreateMultipleConfigurationsBasedOnDebeziumAndQuarkusDatasource() {
        when(quarkusNotificationChannel.name()).thenReturn("a_name");

        var underTest = new PostgresEngineProducer(mock(DefaultStateHandler.class),
                Map.of(
                        "default",
                        new PostgresDatasourceConfiguration(
                                "host",
                                "username",
                                "password",
                                "database",
                                "1926",
                                true,
                                "<default>"),
                        "another",
                        new PostgresDatasourceConfiguration(
                                "another_host",
                                "another_username",
                                "another_password",
                                "another_database",
                                "1926",
                                false,
                                "another")),
                quarkusNotificationChannel,
                event -> {
                    /* ignore */ });

        DebeziumConnectorRegistry registry = underTest.engine(new DebeziumEngineConfiguration() {
            @Override
            public Map<String, String> defaultConfiguration() {
                return Collections.emptyMap();
            }

            @Override
            public Map<String, Capturing> capturing() {
                return Map.of("default", new Capturing() {
                    @Override
                    public Optional<String> groupId() {
                        return Optional.of("default");
                    }

                    @Override
                    public Optional<String> destination() {
                        return Optional.empty();
                    }

                    @Override
                    public Optional<String> deserializer() {
                        return Optional.empty();
                    }

                    @Override
                    public Map<String, String> configurations() {
                        return Map.of("configuration.key", "default_value");
                    }
                }, "another", new Capturing() {
                    @Override
                    public Optional<String> groupId() {
                        return Optional.of("another");
                    }

                    @Override
                    public Optional<String> destination() {
                        return Optional.empty();
                    }

                    @Override
                    public Optional<String> deserializer() {
                        return Optional.empty();
                    }

                    @Override
                    public Map<String, String> configurations() {
                        return Map.of("configuration.key", "another_value");
                    }
                });
            }
        });

        assertThat(registry.get(new CaptureGroup("default"))
                .configuration())
                .isEqualTo(Map.of(
                        "configuration.key", "default_value",
                        "connector.class", "io.debezium.connector.postgresql.PostgresConnector",
                        "name", "default",
                        "database.hostname", "host",
                        "database.port", "1926",
                        "database.user", "username",
                        "database.password", "password",
                        "notification.enabled.channels", "a_name",
                        "database.dbname", "database"));

        assertThat(registry.get(new CaptureGroup("another"))
                .configuration())
                .isEqualTo(Map.of(
                        "configuration.key", "another_value",
                        "connector.class", "io.debezium.connector.postgresql.PostgresConnector",
                        "name", "another",
                        "database.hostname", "another_host",
                        "database.port", "1926",
                        "database.user", "another_username",
                        "database.password", "another_password",
                        "notification.enabled.channels", "a_name",
                        "database.dbname", "another_database"));
    }
}
