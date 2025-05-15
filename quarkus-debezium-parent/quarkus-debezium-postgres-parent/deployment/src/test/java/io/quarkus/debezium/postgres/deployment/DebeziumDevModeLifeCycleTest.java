/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.postgres.deployment;

import static io.quarkus.debezium.engine.PostgresEngineProducer.POSTGRES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.given;

import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;

import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumStatus;
import io.quarkus.runtime.Application;
import io.quarkus.test.QuarkusUnitTest;

public class DebeziumDevModeLifeCycleTest {

    @Inject
    Debezium debezium;

    @RegisterExtension
    static final QuarkusUnitTest application = new QuarkusUnitTest()
            .setArchiveProducer(() -> ShrinkWrap.create(JavaArchive.class))
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
            .overrideConfigKey("quarkus.debezium.name", "test")
            .overrideConfigKey("quarkus.debezium.topic.prefix", "dbserver1")
            .overrideConfigKey("quarkus.debezium.table.include.list", "inventory.products")
            .overrideConfigKey("quarkus.debezium.plugin.name", "pgoutput")
            .overrideConfigKey("quarkus.debezium.snapshot.mode", "never")
            .setLogRecordPredicate(record -> record.getLoggerName().equals("io.quarkus.debezium.engine.DebeziumRunner"))
            .assertLogRecords((records) -> {
                assertThat(records.getFirst().getMessage()).isEqualTo("Starting Debezium Engine...");
                assertThat(records.get(1).getMessage()).isEqualTo("Shutting down Debezium Engine...");
            });

    @Test
    @DisplayName("debezium should be integrated in the quarkus dev lifecycle")
    void shouldDebeziumBeIntegratedInTheQuarkusDevLifeCycle() {
        assertThat(debezium.configuration().get("connector.class"))
                .isEqualTo("io.debezium.connector.postgresql.PostgresConnector");

        given().await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(debezium.status())
                        .isEqualTo(new DebeziumStatus(DebeziumStatus.State.POLLING)));

        assertThat(debezium.connector()).isEqualTo(POSTGRES);

        Application.currentApplication().close();

        given().await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(debezium.status())
                        .isEqualTo(new DebeziumStatus(DebeziumStatus.State.STOPPED)));
    }
}
