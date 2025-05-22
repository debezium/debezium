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

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.engine.RecordChangeEvent;
import io.debezium.runtime.Capturing;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumStatus;
import io.quarkus.test.QuarkusUnitTest;
import io.quarkus.test.common.QuarkusTestResource;

@QuarkusTestResource(value = DatabaseTestResource.class)
public class CapturingTest {

    @Inject
    CaptureProductsHandler captureProductsHandler;

    @Inject
    Debezium debezium;

    @BeforeEach
    void setUp() {
        given().await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(debezium.status())
                        .isEqualTo(new DebeziumStatus(DebeziumStatus.State.POLLING)));
    }

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar
                    .addClasses(CaptureProductsHandler.class))
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
            .overrideConfigKey("quarkus.debezium.name", "test")
            .overrideConfigKey("quarkus.debezium.topic.prefix", "dbserver1")
            .overrideConfigKey("quarkus.debezium.plugin.name", "pgoutput")
            .overrideConfigKey("quarkus.debezium.snapshot.mode", "initial")
            .overrideConfigKey("quarkus.datasource.devservices.enabled", "false");

    @Test
    @DisplayName("should invoke the default capture")
    void shouldInvokeDefaultCapture() {
        given().await()
                .atMost(100, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(captureProductsHandler.isInvoked()).isTrue());

    }

    @ApplicationScoped
    static class CaptureProductsHandler {
        private final AtomicBoolean invoked = new AtomicBoolean(false);

        @Capturing()
        public void capture(RecordChangeEvent<SourceRecord> event) {
            invoked.set(true);
        }

        public boolean isInvoked() {
            return invoked.getAndSet(false);
        }
    }

}
