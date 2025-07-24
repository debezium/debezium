/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.postgres.deployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.given;

import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;

import org.assertj.core.api.Assertions;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumStatus;
import io.quarkus.runtime.Application;
import io.quarkus.test.QuarkusUnitTest;
import io.quarkus.test.common.QuarkusTestResource;

@QuarkusTestResource(value = DatabaseTestResource.class)
public class DebeziumLifeCycleTest {

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
            .overrideConfigKey("quarkus.debezium.snapshot.mode", "no_data")
            .overrideConfigKey("quarkus.datasource.devservices.enabled", "false")
            .setLogRecordPredicate(record -> record.getLoggerName().equals("io.quarkus.debezium.engine.DebeziumRunner"))
            .assertLogRecords((records) -> {
                assertThat(records.getFirst().getMessage()).isEqualTo("Starting Debezium Engine...");
                assertThat(records.get(1).getMessage()).isEqualTo("Shutting down Debezium Engine...");
            });

    @Test
    @DisplayName("debezium should be integrated in the quarkus lifecycle")
    void shouldDebeziumBeIntegratedInTheQuarkusLifeCycle() {
        Assertions.assertThat(debezium.configuration().get("connector.class"))
                .isEqualTo("io.debezium.connector.postgresql.PostgresConnector");

        given().await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertThat(debezium.status())
                        .isEqualTo(new DebeziumStatus(DebeziumStatus.State.POLLING)));

        Application.currentApplication().close();

        given().await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertThat(debezium.status())
                        .isEqualTo(new DebeziumStatus(DebeziumStatus.State.STOPPED)));
    }
}
