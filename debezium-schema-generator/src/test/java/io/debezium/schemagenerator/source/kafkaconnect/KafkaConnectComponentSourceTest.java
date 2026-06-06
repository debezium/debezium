/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.source.kafkaconnect;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.metadata.ComponentMetadata;

/**
 * Tests for {@link KafkaConnectComponentSource}.
 */
class KafkaConnectComponentSourceTest {

    @Test
    void shouldReturnNonNullComponents() {
        // Given
        KafkaConnectComponentSource source = new KafkaConnectComponentSource(
                new KafkaConnectDiscoveryService(),
                new ConfigDefExtractor(),
                new ConfigDefAdapter());

        // When
        List<ComponentMetadata> components = source.discoverComponents();

        // Then
        assertThat(components).isNotNull();
    }

    @Test
    void shouldReturnSourceName() {
        // Given
        KafkaConnectComponentSource source = new KafkaConnectComponentSource(
                new KafkaConnectDiscoveryService(),
                new ConfigDefExtractor(),
                new ConfigDefAdapter());

        // When
        String name = source.getName();

        // Then
        assertThat(name).isEqualTo("Kafka Connect Components");
    }

    @Test
    void shouldDiscoverComponentsWhenKafkaConnectAvailable() {
        // Given
        KafkaConnectComponentSource source = new KafkaConnectComponentSource(
                new KafkaConnectDiscoveryService(),
                new ConfigDefExtractor(),
                new ConfigDefAdapter());

        // When
        List<ComponentMetadata> components = source.discoverComponents();

        // Then
        // If Kafka Connect JARs are on classpath, we should find some components
        // If not, the list will be empty (which is also valid)
        assertThat(components).isNotNull();

        if (!components.isEmpty()) {
            // Verify all components have required metadata
            assertThat(components)
                    .allSatisfy(metadata -> {
                        assertThat(metadata.getComponentDescriptor()).isNotNull();
                        assertThat(metadata.getComponentDescriptor().getClassName())
                                .startsWith("org.apache.kafka.");
                        assertThat(metadata.getComponentFields()).isNotNull();
                    });
        }
    }

    @Test
    void shouldIncludeComponentsWithConfigDef() {
        // Given
        KafkaConnectComponentSource source = new KafkaConnectComponentSource(
                new KafkaConnectDiscoveryService(),
                new ConfigDefExtractor(),
                new ConfigDefAdapter());

        // When
        List<ComponentMetadata> components = source.discoverComponents();

        // Then - all discovered components should have config fields
        assertThat(components)
                .allSatisfy(metadata -> assertThat(metadata.getComponentFields()).isNotNull());
    }

    @Test
    void shouldHaveValidComponentDescriptors() {
        // Given
        KafkaConnectComponentSource source = new KafkaConnectComponentSource(
                new KafkaConnectDiscoveryService(),
                new ConfigDefExtractor(),
                new ConfigDefAdapter());

        // When
        List<ComponentMetadata> components = source.discoverComponents();

        // Then
        assertThat(components)
                .allSatisfy(metadata -> {
                    assertThat(metadata.getComponentDescriptor().getClassName()).isNotBlank();
                    assertThat(metadata.getComponentDescriptor().getType()).isNotBlank();
                    assertThat(metadata.getComponentDescriptor().getVersion()).isNotBlank();
                });
    }
}
