/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.mongodb.deployment.suite;

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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumStatus;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.PostProcessing;
import io.quarkus.debezium.mongodb.deployment.SuiteTags;
import io.quarkus.test.QuarkusUnitTest;

@Tag(SuiteTags.DEFAULT)
public class PostProcessingTest {

    @Inject
    PostProcessingHandler postProcessingHandler;

    @Inject
    DebeziumConnectorRegistry debeziumConnectorRegistry;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(PostProcessingHandler.class))
            .withConfigurationResource("quarkus-debezium-testsuite.properties");

    @BeforeEach
    void setUp() {
        given().await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(debeziumConnectorRegistry.get(new EngineManifest("default")).status())
                        .isEqualTo(new DebeziumStatus(DebeziumStatus.State.POLLING)));
    }

    @Test
    @DisplayName("should use post processor")
    void shouldApplyPostProcessor() {
        given().await()
                .atMost(100, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(postProcessingHandler.key()).contains("id"));
    }

    @ApplicationScoped
    static class PostProcessingHandler {
        private final List<String> ids = new ArrayList<>();

        @PostProcessing()
        public void postProcessing(Object key, Struct struct) {
            this.ids.add(((Struct) key)
                    .getString("id"));
        }

        public String key() {
            return ids.isEmpty() ? "" : ids.getFirst();
        }
    }
}
