/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.openlineage.emitter.LineageEmitter;
import io.debezium.openlineage.emitter.NoOpLineageEmitter;
import io.debezium.openlineage.emitter.OpenLineageEmitter;

public class DebeziumLineageEmitterFactoryTest {

    private DebeziumLineageEmitterFactory factory;

    @BeforeEach
    void setUp() {
        factory = new DebeziumLineageEmitterFactory();
    }

    @Test
    void shouldReturnNoOpEmitterWhenOpenLineageDisabled() {

        Map<String, String> config = Map.of(
                "name", "test-connector",
                "openlineage.integration.enabled", "false");

        LineageEmitter emitter = factory.get(new ConnectorContext("test-connector", "mysql", "0", "3.2.0.Final", UUID.randomUUID(), config));

        assertNotNull(emitter);
        assertTrue(emitter instanceof NoOpLineageEmitter);
    }

    @Test
    void shouldReturnOpenLineageEmitterWhenEnabled() {

        Map<String, String> config = Map.of(
                "name", "test-connector",
                "openlineage.integration.enabled", "true",
                "openlineage.integration.config.file.path",
                DebeziumLineageEmitterFactoryTest.class.getClassLoader().getResource("openlineage/openlineage.yml").getPath(),
                "openlineage.integration.job.namespace", "test-namespace",
                "openlineage.integration.job.description", "test-desc",
                "openlineage.integration.job.tags", "tag1=value1",
                "openlineage.integration.job.owners", "owner1=teamA");

        LineageEmitter emitter = factory.get(new ConnectorContext("test-connector", "mysql", "0", "3.2.0.Final", UUID.randomUUID(), config));

        assertNotNull(emitter);
        assertTrue(emitter instanceof OpenLineageEmitter);
    }
}
