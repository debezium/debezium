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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.RecordChangeEvent;
import io.debezium.runtime.Capturing;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumStatus;
import io.quarkus.test.QuarkusUnitTest;
import io.quarkus.test.common.QuarkusTestResource;

@QuarkusTestResource(value = DatabaseTestResource.class, restrictToAnnotatedClass = true)
public class CapturingTest {
    private static final Logger logger = LoggerFactory.getLogger(CapturingTest.class);

    @Inject
    CaptureProductsHandler productsHandler;

    @Inject
    MultipleCaptureHandler multipleCaptureHandler;

    @Inject
    GeneralCaptureHandler generalCaptureHandler;

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
                    .addClasses(CaptureProductsHandler.class, MultipleCaptureHandler.class, GeneralCaptureHandler.class))
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
            .overrideConfigKey("quarkus.debezium.name", "test")
            .overrideConfigKey("quarkus.debezium.topic.prefix", "dbserver1")
            .overrideConfigKey("quarkus.debezium.plugin.name", "pgoutput")
            .overrideConfigKey("quarkus.debezium.snapshot.mode", "initial")
            .overrideConfigKey("quarkus.hibernate-orm.database.generation", "drop-and-create")
            .setLogRecordPredicate(record -> record.getLoggerName().equals("io.quarkus.debezium.postgres.deployment.CapturingTest"))
            .assertLogRecords((records) -> {
                assertThat(records.getFirst().getMessage()).isEqualTo("should be not removed even if it's not injected in any bean");
            });

    @Test
    @DisplayName("should invoke the capture method annotated with product qualifier")
    void shouldInvokeTheCaptureAnnotation() {
        given().await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(productsHandler.isInvoked()).isTrue());
    }

    @Test
    @DisplayName("should invoke multiple capture methods annotated")
    void shouldInvokeProductAndOrders() {
        given().await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(multipleCaptureHandler.isUsersInvoked()).isTrue());

        given().await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(multipleCaptureHandler.isOrdersInvoked()).isTrue());
    }

    @Test
    @DisplayName("should invoke the default capture and never a capture with wrong qualifier")
    void shouldInvokeDefaultCaptureAndNeverWhenWrongQualifier() {
        given().await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(generalCaptureHandler.isDefaultInvoked()).isTrue());

        given().await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(generalCaptureHandler.isNoExistInvoked()).isFalse());
    }

    @ApplicationScoped
    static class CaptureProductsHandler {
        private final AtomicBoolean invoked = new AtomicBoolean(false);

        @Capturing("public.products")
        public void capture(RecordChangeEvent<SourceRecord> event) {
            invoked.set(true);
        }

        public boolean isInvoked() {
            return invoked.getAndSet(false);
        }
    }

    @ApplicationScoped
    static class NoInjectHandler {
        @Capturing("public.injected")
        public void captureOrders(RecordChangeEvent<SourceRecord> event) {
            logger.info("should be not removed even if it's not injected in any bean");
        }
    }

    @ApplicationScoped
    static class MultipleCaptureHandler {
        private final AtomicBoolean invokedOrders = new AtomicBoolean(false);
        private final AtomicBoolean invokedUsers = new AtomicBoolean(false);

        @Capturing("public.orders")
        public void captureOrders(RecordChangeEvent<SourceRecord> event) {
            invokedOrders.set(true);
        }

        @Capturing("public.users")
        public void captureUsers(RecordChangeEvent<SourceRecord> event) {
            invokedUsers.set(true);
        }

        public boolean isOrdersInvoked() {
            return invokedOrders.getAndSet(false);
        }

        public boolean isUsersInvoked() {
            return invokedUsers.getAndSet(false);
        }
    }

    @ApplicationScoped
    static class GeneralCaptureHandler {
        private final AtomicBoolean qualifierNotExists = new AtomicBoolean(false);
        private final AtomicBoolean defaultQualifier = new AtomicBoolean(false);

        @Capturing("public.not_exists")
        public void noExist(RecordChangeEvent<SourceRecord> event) {
            qualifierNotExists.set(true);
        }

        @Capturing()
        public void defaultQualifier(RecordChangeEvent<SourceRecord> event) {
            defaultQualifier.set(true);
        }

        public boolean isNoExistInvoked() {
            return qualifierNotExists.getAndSet(false);
        }

        public boolean isDefaultInvoked() {
            return defaultQualifier.getAndSet(false);
        }
    }

}
