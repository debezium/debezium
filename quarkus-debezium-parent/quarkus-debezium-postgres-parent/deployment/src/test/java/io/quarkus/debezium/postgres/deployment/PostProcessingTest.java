/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.postgres.deployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.given;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.CaptureGroup;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumStatus;
import io.debezium.runtime.PostProcessing;
import io.quarkus.test.QuarkusUnitTest;
import io.quarkus.test.common.QuarkusTestResource;

@QuarkusTestResource(value = DatabaseTestResource.class)
public class PostProcessingTest {

    @Inject
    PostProcessingHandler postProcessingHandler;

    @Inject
    DebeziumConnectorRegistry debeziumConnectorRegistry;

    @BeforeEach
    void setUp() {
        given().await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(debeziumConnectorRegistry.get(new CaptureGroup("default")).status())
                        .isEqualTo(new DebeziumStatus(DebeziumStatus.State.POLLING)));
    }

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar
                    .addClasses(PostProcessingTest.PostProcessingHandler.class))
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
            .overrideConfigKey("quarkus.debezium.name", "test")
            .overrideConfigKey("quarkus.debezium.topic.prefix", "dbserver1")
            .overrideConfigKey("quarkus.debezium.plugin.name", "pgoutput")
            .overrideConfigKey("quarkus.debezium.snapshot.mode", "initial")
            .overrideConfigKey("quarkus.datasource.devservices.enabled", "false");

    @Test
    @DisplayName("should use post processor")
    void shouldApplyPostProcessor() {
        given().await()
                .atMost(100, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(postProcessingHandler.key()).isEqualTo(1));

    }

    @ApplicationScoped
    static class PostProcessingHandler {
        private final List<Integer> ids = new ArrayList<>();

        @PostProcessing()
        public void postProcessing(Object key, Struct struct) {
            this.ids.add(((Struct) key)
                    .getInt32("id"));
        }

        public int key() {
            return ids.isEmpty() ? 0 : ids.getFirst();
        }
    }
}
