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
import org.assertj.core.api.Assertions;
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
    Capture capture;

    @Inject
    Debezium debezium;

    @RegisterExtension
    static final QuarkusUnitTest config = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar
                    .addClasses(Capture.class))
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
            .overrideConfigKey("quarkus.debezium.name", "test")
            .overrideConfigKey("quarkus.debezium.topic.prefix", "dbserver1")
            .overrideConfigKey("quarkus.debezium.table.include.list", "public.product")
            .overrideConfigKey("quarkus.debezium.plugin.name", "pgoutput")
            .overrideConfigKey("quarkus.debezium.snapshot.mode", "initial")
            .overrideConfigKey("quarkus.hibernate-orm.database.generation", "drop-and-create")
            .setLogRecordPredicate(record -> record.getLoggerName().equals("io.quarkus.debezium.postgres.deployment.CapturingTest"))
            .assertLogRecords((records) -> assertThat(records.getFirst().getMessage()).isEqualTo("here to stay!"));

    @Test
    @DisplayName("should invoke the capture method annotated with product")
    void shouldInvokeTheCaptureAnnotation() {
        Assertions.assertThat(debezium.configuration().get("connector.class"))
                .isEqualTo("io.debezium.connector.postgresql.PostgresConnector");

        given().await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertThat(debezium.status())
                        .isEqualTo(new DebeziumStatus(DebeziumStatus.State.POLLING)));

        given().await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertThat(capture.isInvoked()).isTrue());
    }

    @ApplicationScoped
    static class Capture {
        private final AtomicBoolean invoked = new AtomicBoolean(false);

        @Capturing("product")
        public void capture(RecordChangeEvent<SourceRecord> event) {
            logger.info("here to stay!");
            invoked.set(true);
        }

        public boolean isInvoked() {
            return invoked.get();
        }
    }
}
