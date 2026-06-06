/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.source.kafkaconnect;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ConfigDefExtractor}.
 */
class ConfigDefExtractorTest {

    @Test
    void shouldExtractFromStaticField() {
        // Given
        ConfigDefExtractor extractor = new ConfigDefExtractor();

        // When
        Optional<ConfigDef> configDef = extractor.extractConfigDef(ComponentWithStaticField.class);

        // Then
        assertThat(configDef).isPresent();
        assertThat(configDef.get().names()).contains("test.config");
    }

    @Test
    void shouldExtractFromConfigMethod() {
        // Given
        ConfigDefExtractor extractor = new ConfigDefExtractor();

        // When
        Optional<ConfigDef> configDef = extractor.extractConfigDef(ComponentWithConfigMethod.class);

        // Then
        assertThat(configDef).isPresent();
        assertThat(configDef.get().names()).contains("method.config");
    }

    @Test
    void shouldReturnEmptyConfigDefWhenNothingFound() {
        // Given
        ConfigDefExtractor extractor = new ConfigDefExtractor();

        // When
        Optional<ConfigDef> configDef = extractor.extractConfigDef(ComponentWithoutConfig.class);

        // Then
        assertThat(configDef).isEmpty();
    }

    @Test
    void shouldPreferStaticFieldOverConfigMethod() {
        // Given
        ConfigDefExtractor extractor = new ConfigDefExtractor();

        // When
        Optional<ConfigDef> configDef = extractor.extractConfigDef(ComponentWithBoth.class);

        // Then - should use static field, not config() method
        assertThat(configDef).isPresent();
        assertThat(configDef.get().names()).contains("static.field");
        assertThat(configDef.get().names()).doesNotContain("config.method");
    }

    // Test helper classes

    public static class ComponentWithStaticField {
        public static final ConfigDef CONFIG_DEF = new ConfigDef()
                .define("test.config", ConfigDef.Type.STRING, "default", ConfigDef.Importance.HIGH, "Test config");
    }

    public static class ComponentWithConfigMethod {
        public ConfigDef config() {
            return new ConfigDef()
                    .define("method.config", ConfigDef.Type.STRING, "default", ConfigDef.Importance.HIGH, "Method config");
        }
    }

    public static class ComponentWithoutConfig {
        // No CONFIG_DEF and no config() method
    }

    public static class ComponentWithBoth {
        public static final ConfigDef CONFIG_DEF = new ConfigDef()
                .define("static.field", ConfigDef.Type.STRING, "default", ConfigDef.Importance.HIGH, "Static field");

        public ConfigDef config() {
            return new ConfigDef()
                    .define("config.method", ConfigDef.Type.STRING, "default", ConfigDef.Importance.HIGH, "Config method");
        }
    }
}
