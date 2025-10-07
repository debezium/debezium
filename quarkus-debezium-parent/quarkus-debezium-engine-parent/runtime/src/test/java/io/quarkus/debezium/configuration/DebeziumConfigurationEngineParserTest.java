/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.debezium.runtime.configuration.DevServicesConfig;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser.MultiEngineConfiguration;

class DebeziumConfigurationEngineParserTest {

    public static final Map<String, String> DEBEZIUM_CONFIGURATION = Map.of(
            "quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore",
            "quarkus.debezium.name", "native",
            "quarkus.debezium.topic.prefix", "dbserver1",
            "quarkus.debezium.plugin.name", "pgoutput",
            "quarkus.debezium.snapshot.mode", "initial");
    private final DebeziumConfigurationEngineParser underTest = new DebeziumConfigurationEngineParser();

    @Test
    @DisplayName("should parse the unnamed default configuration")
    void shouldParseDefaultConfiguration() {
        List<MultiEngineConfiguration> configurations = underTest.parse(new DebeziumEngineConfiguration() {
            @Override
            public Map<String, String> defaultConfiguration() {
                return DEBEZIUM_CONFIGURATION;
            }

            @Override
            public Map<String, Capturing> capturing() {
                return Map.of(
                        "product", new Capturing() {
                            @Override
                            public Optional<String> engineId() {
                                return Optional.empty();
                            }

                            @Override
                            public Optional<String> destination() {
                                return Optional.of("aDestination");
                            }

                            @Override
                            public Optional<String> deserializer() {
                                return Optional.of("deserializer");
                            }

                            @Override
                            public Map<String, DeserializerConfiguration> deserializers() {
                                return Map.of();
                            }

                            @Override
                            public Map<String, String> configurations() {
                                return Map.of();
                            }
                        });
            }

            @Override
            public Map<String, DevServicesConfig> devservices() {
                return Map.of();
            }
        });

        assertThat(configurations).containsOnly(new MultiEngineConfiguration("default", DEBEZIUM_CONFIGURATION));
    }

    @Test
    @DisplayName("should parse named default configuration")
    void shouldParseNamedDefaultConfiguration() {
        List<MultiEngineConfiguration> actual = underTest.parse(new DebeziumEngineConfiguration() {
            @Override
            public Map<String, String> defaultConfiguration() {
                return Map.of();
            }

            @Override
            public Map<String, Capturing> capturing() {
                return Map.of("default", capturingWithEngineId("default"));
            }

            @Override
            public Map<String, DevServicesConfig> devservices() {
                return Map.of();
            }
        });

        assertThat(actual).containsOnly(new MultiEngineConfiguration("default", DEBEZIUM_CONFIGURATION));
    }

    @Test
    @DisplayName("should not contains two default configuration")
    void shouldNotContainsTwoDefaultConfiguration() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> underTest.parse(new DebeziumEngineConfiguration() {
            @Override
            public Map<String, String> defaultConfiguration() {
                return DEBEZIUM_CONFIGURATION;
            }

            @Override
            public Map<String, Capturing> capturing() {
                return Map.of(
                        "default", capturingWithEngineId("default"),
                        "another", capturingWithEngineId("another"));
            }

            @Override
            public Map<String, DevServicesConfig> devservices() {
                return Map.of();
            }
        }));
    }

    @Test
    @DisplayName("should parse multiple configurations")
    void shouldParseMultipleConfigurations() {
        List<MultiEngineConfiguration> actual = underTest.parse(new DebeziumEngineConfiguration() {
            @Override
            public Map<String, String> defaultConfiguration() {
                return Map.of();
            }

            @Override
            public Map<String, Capturing> capturing() {
                return Map.of(
                        "orders", capturingWithEngineId("orders"),
                        "products", capturingWithEngineId("products"));
            }

            @Override
            public Map<String, DevServicesConfig> devservices() {
                return Map.of();
            }
        });

        assertThat(actual).containsExactlyInAnyOrder(new MultiEngineConfiguration("products", DEBEZIUM_CONFIGURATION),
                new MultiEngineConfiguration("orders", DEBEZIUM_CONFIGURATION));
    }

    @Test
    @DisplayName("should parse multiple configurations with a default")
    void shouldParseMultipleWithDefaultConfigurations() {
        List<MultiEngineConfiguration> actual = underTest.parse(new DebeziumEngineConfiguration() {
            @Override
            public Map<String, String> defaultConfiguration() {
                return Map.of();
            }

            @Override
            public Map<String, Capturing> capturing() {
                return Map.of(
                        "orders", capturingWithEngineId("orders"),
                        "default", capturingWithEngineId("default"), "product", capturingWithEngineId("products"));
            }

            @Override
            public Map<String, DevServicesConfig> devservices() {
                return Map.of();
            }

        });

        assertThat(actual).containsExactlyInAnyOrder(new MultiEngineConfiguration("products", DEBEZIUM_CONFIGURATION),
                new MultiEngineConfiguration("orders", DEBEZIUM_CONFIGURATION),
                new MultiEngineConfiguration("default", DEBEZIUM_CONFIGURATION));
    }

    @Test
    @DisplayName("should parse multiple configurations with an unnamed default")
    void shouldParseMultipleWithUnnamedDefaultConfigurations() {
        List<MultiEngineConfiguration> actual = underTest.parse(new DebeziumEngineConfiguration() {
            @Override
            public Map<String, String> defaultConfiguration() {
                return DEBEZIUM_CONFIGURATION;
            }

            @Override
            public Map<String, Capturing> capturing() {
                return Map.of(
                        "orders", capturingWithEngineId("orders"), "product", capturingWithEngineId("products"));
            }

            @Override
            public Map<String, DevServicesConfig> devservices() {
                return Map.of();
            }

        });

        assertThat(actual).containsExactlyInAnyOrder(new MultiEngineConfiguration("products", DEBEZIUM_CONFIGURATION),
                new MultiEngineConfiguration("orders", DEBEZIUM_CONFIGURATION),
                new MultiEngineConfiguration("default", DEBEZIUM_CONFIGURATION));
    }

    private DebeziumEngineConfiguration.Capturing capturingWithEngineId(String products) {
        return new DebeziumEngineConfiguration.Capturing() {
            @Override
            public Optional<String> engineId() {
                return Optional.of(products);
            }

            @Override
            public Optional<String> destination() {
                return Optional.of("aDestination");
            }

            @Override
            public Optional<String> deserializer() {
                return Optional.of("deserializer");
            }

            @Override
            public Map<String, DebeziumEngineConfiguration.DeserializerConfiguration> deserializers() {
                return Map.of();
            }

            @Override
            public Map<String, String> configurations() {
                return DEBEZIUM_CONFIGURATION;
            }

        };
    }
}
