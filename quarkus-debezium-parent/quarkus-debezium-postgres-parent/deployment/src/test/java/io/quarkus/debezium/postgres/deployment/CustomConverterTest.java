/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.postgres.deployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.given;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.relational.CustomConverterRegistry.ConverterDefinition;
import io.debezium.runtime.CustomConverter;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumStatus;
import io.debezium.runtime.FieldFilterStrategy;
import io.debezium.spi.converter.ConvertedField;
import io.quarkus.test.QuarkusUnitTest;
import io.quarkus.test.common.QuarkusTestResource;

@QuarkusTestResource(value = DatabaseTestResource.class)
public class CustomConverterTest {

    @Inject
    CustomConverterBinder customConverterBinder;

    @Inject
    CustomFieldFilterStrategy fieldFilterStrategy;

    @Inject
    Debezium debezium;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar
                    .addClasses(CustomConverterTest.CustomConverterBinder.class))
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
            .overrideConfigKey("quarkus.debezium.name", "test")
            .overrideConfigKey("quarkus.debezium.topic.prefix", "dbserver1")
            .overrideConfigKey("quarkus.debezium.plugin.name", "pgoutput")
            .overrideConfigKey("quarkus.debezium.snapshot.mode", "initial")
            .overrideConfigKey("quarkus.datasource.devservices.enabled", "false");

    @BeforeEach
    void setUp() {
        given().await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(debezium.status())
                        .isEqualTo(new DebeziumStatus(DebeziumStatus.State.POLLING)));
    }

    @Test
    @DisplayName("should apply custom converter")
    void shouldApplyCustomConverter() {
        given().await()
                .atMost(100, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(customConverterBinder.isInvoked()).isTrue());
    }

    @Test
    @DisplayName("should evaluate filter strategy for custom converter if defined")
    void shouldUseFilterWhenDefinedInCustomConverter() {
        given().await()
                .atMost(100, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(fieldFilterStrategy.isInvoked()).isTrue());
    }

    @ApplicationScoped
    public static class CustomConverterBinder {
        private final AtomicBoolean invoked = new AtomicBoolean(false);

        @CustomConverter
        public ConverterDefinition<SchemaBuilder> bind(ConvertedField field) {
            invoked.set(true);
            return new ConverterDefinition<>(SchemaBuilder.string(), String::valueOf);
        }

        @CustomConverter(filter = CustomFieldFilterStrategy.class)
        public ConverterDefinition<SchemaBuilder> filteredBind(ConvertedField field) {
            return new ConverterDefinition<>(SchemaBuilder.string(), String::valueOf);
        }

        public boolean isInvoked() {
            return invoked.getAndSet(false);
        }
    }

    @ApplicationScoped
    public static class CustomFieldFilterStrategy implements FieldFilterStrategy {
        private final AtomicBoolean invoked = new AtomicBoolean(false);

        @Override
        public boolean filter(ConvertedField field) {
            invoked.set(true);
            return false;
        }

        public boolean isInvoked() {
            return invoked.getAndSet(false);
        }
    }
}
