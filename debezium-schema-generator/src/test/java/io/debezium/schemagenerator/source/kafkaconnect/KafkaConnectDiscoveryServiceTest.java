/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.source.kafkaconnect;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link KafkaConnectDiscoveryService}.
 */
class KafkaConnectDiscoveryServiceTest {

    private static final List<Class<?>> SUPPORTED_INTERFACES = List.of(
            Transformation.class, Predicate.class, Converter.class, HeaderConverter.class);

    @Test
    void shouldReturnNonNullResults() {
        // Given
        KafkaConnectDiscoveryService service = new KafkaConnectDiscoveryService();

        // When
        List<Class<?>> components = service.discoverKafkaConnectComponents();

        // Then
        assertThat(components).isNotNull();
    }

    @Test
    void shouldOnlyDiscoverKafkaConnectComponents() {
        // Given
        KafkaConnectDiscoveryService service = new KafkaConnectDiscoveryService();

        // When
        List<Class<?>> components = service.discoverKafkaConnectComponents();

        // Then - only components from org.apache.kafka are reported
        assertThat(components)
                .allSatisfy(clazz -> assertThat(clazz.getName())
                        .startsWith("org.apache.kafka."));
    }

    @Test
    void shouldOnlyDiscoverSupportedComponentKinds() {
        // Given
        KafkaConnectDiscoveryService service = new KafkaConnectDiscoveryService();

        // When
        List<Class<?>> components = service.discoverKafkaConnectComponents();

        // Then - ConnectPlugin also covers connectors and config providers, which are not wanted
        assertThat(components)
                .allSatisfy(clazz -> assertThat(SUPPORTED_INTERFACES)
                        .as("%s should be a transformation, predicate, converter or header converter", clazz.getName())
                        .anyMatch(componentInterface -> componentInterface.isAssignableFrom(clazz)));
    }

    @Test
    void shouldReportEachComponentOnce() {
        // Given
        KafkaConnectDiscoveryService service = new KafkaConnectDiscoveryService();

        // When
        List<Class<?>> components = service.discoverKafkaConnectComponents();

        // Then - components matching several interfaces, such as converters that are also
        // header converters, must not be reported more than once
        assertThat(components).doesNotHaveDuplicates();
    }

    @Test
    void shouldOnlyDiscoverConcreteClasses() {
        // Given
        KafkaConnectDiscoveryService service = new KafkaConnectDiscoveryService();

        // When
        List<Class<?>> components = service.discoverKafkaConnectComponents();

        // Then - no abstract classes should be discovered
        assertThat(components)
                .allSatisfy(clazz -> assertThat(clazz)
                        .matches(c -> !java.lang.reflect.Modifier.isAbstract(c.getModifiers()),
                                "should not be abstract"));
    }
}
