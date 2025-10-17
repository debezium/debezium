/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.testsuite.deployment.suite;

import static org.awaitility.Awaitility.given;

import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;

import org.assertj.core.api.Assertions;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumStatus;
import io.debezium.runtime.EngineManifest;
import io.quarkus.debezium.testsuite.deployment.SuiteTags;
import io.quarkus.debezium.testsuite.deployment.TestSuiteConfigurations;
import io.quarkus.runtime.Application;
import io.quarkus.test.QuarkusUnitTest;

@Tag(SuiteTags.DEFAULT)
public class DebeziumLifeCycleTest {

    @Inject
    DebeziumConnectorRegistry registry;

    @RegisterExtension
    static final QuarkusUnitTest application = new QuarkusUnitTest()
            .setArchiveProducer(() -> ShrinkWrap.create(JavaArchive.class))
            .withConfigurationResource("quarkus-debezium-testsuite.properties");

    @Test
    @DisplayName("debezium should be integrated in the quarkus lifecycle")
    void shouldDebeziumBeIntegratedInTheQuarkusLifeCycle() {
        Assertions.assertThat(registry.get(new EngineManifest("default")).configuration().get("connector.class"))
                .contains("io.debezium.connector");

        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertThat(registry.get(new EngineManifest("default")).status())
                        .isEqualTo(new DebeziumStatus(DebeziumStatus.State.POLLING)));

        Application.currentApplication().close();

        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertThat(registry.get(new EngineManifest("default")).status())
                        .isEqualTo(new DebeziumStatus(DebeziumStatus.State.STOPPED)));
    }
}
