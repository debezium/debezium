/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.source.kafkaconnect;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link KafkaConnectDiscoveryService}.
 */
class KafkaConnectDiscoveryServiceTest {

    @Test
    void shouldReturnNonNullResults() {
        // Given
        KafkaConnectDiscoveryService service = new KafkaConnectDiscoveryService();

        // When
        Map<ComponentType, List<Class<?>>> components = service.discoverKafkaConnectComponents();

        // Then
        assertThat(components).isNotNull();
    }

    @Test
    void shouldIncludeAllComponentTypes() {
        // Given
        KafkaConnectDiscoveryService service = new KafkaConnectDiscoveryService();

        // When
        Map<ComponentType, List<Class<?>>> components = service.discoverKafkaConnectComponents();

        // Then - all component types should be present (even if empty)
        assertThat(components).containsKeys(
                ComponentType.TRANSFORMATION,
                ComponentType.CONVERTER,
                ComponentType.HEADER_CONVERTER,
                ComponentType.PREDICATE);
    }

    @Test
    void shouldDiscoverTransformationsWhenKafkaConnectAvailable() {
        // Given
        KafkaConnectDiscoveryService service = new KafkaConnectDiscoveryService();

        // When
        Map<ComponentType, List<Class<?>>> components = service.discoverKafkaConnectComponents();

        // Then
        List<Class<?>> transformations = components.get(ComponentType.TRANSFORMATION);

        // If Kafka Connect JARs are on classpath, we should find some transformations
        // If not, the list will be empty (which is also valid)
        assertThat(transformations).isNotNull();

        if (!transformations.isEmpty()) {
            // Verify discovered classes are from org.apache.kafka
            assertThat(transformations)
                    .allSatisfy(clazz -> assertThat(clazz.getName())
                            .startsWith("org.apache.kafka."));
        }
    }

    @Test
    void shouldOnlyDiscoverConcreteClasses() {
        // Given
        KafkaConnectDiscoveryService service = new KafkaConnectDiscoveryService();

        // When
        Map<ComponentType, List<Class<?>>> components = service.discoverKafkaConnectComponents();

        // Then - no abstract classes should be discovered
        for (List<Class<?>> classList : components.values()) {
            assertThat(classList)
                    .allSatisfy(clazz -> assertThat(clazz)
                            .matches(c -> !java.lang.reflect.Modifier.isAbstract(c.getModifiers()),
                                    "should not be abstract"));
        }
    }
}
