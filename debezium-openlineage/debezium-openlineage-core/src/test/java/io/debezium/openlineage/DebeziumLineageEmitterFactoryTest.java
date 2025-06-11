/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.debezium.openlineage.emitter.LineageEmitter;
import io.debezium.openlineage.emitter.NoOpLineageEmitter;
import io.debezium.openlineage.emitter.OpenLineageEmitter;

public class DebeziumLineageEmitterFactoryTest {

    private DebeziumLineageEmitterFactory factory;

    @Before
    public void setUp() {
        factory = new DebeziumLineageEmitterFactory();
    }

    @Test
    public void shouldReturnNoOpEmitterWhenOpenLineageDisabled() {

        Map<String, String> config = Map.of(
                "name", "test-connector",
                "openlineage.integration.enabled", "false");

        LineageEmitter emitter = factory.get(new ConnectorContext("test-connector", "mysql", "0", "3.2.0.Final", config));

        assertNotNull(emitter);
        assertTrue(emitter instanceof NoOpLineageEmitter);
    }

    @Test
    public void shouldReturnOpenLineageEmitterWhenEnabled() {

        Map<String, String> config = Map.of(
                "name", "test-connector",
                "openlineage.integration.enabled", "true",
                "openlineage.integration.config.file.path",
                DebeziumLineageEmitterFactoryTest.class.getClassLoader().getResource("openlineage/openlineage.yml").getPath(),
                "openlineage.integration.job.namespace", "test-namespace",
                "openlineage.integration.job.description", "test-desc",
                "openlineage.integration.job.tags", "tag1=value1",
                "openlineage.integration.job.owners", "owner1=teamA");

        LineageEmitter emitter = factory.get(new ConnectorContext("test-connector", "mysql", "0", "3.2.0.Final", config));

        assertNotNull(emitter);
        assertTrue(emitter instanceof OpenLineageEmitter);
    }

    @Test
    public void shouldReuseOpenLineageContextAcrossCalls() {

        Map<String, String> config = Map.of(
                "name", "test-connector",
                "openlineage.integration.enabled", "true",
                "openlineage.integration.config.file.path",
                DebeziumLineageEmitterFactoryTest.class.getClassLoader().getResource("openlineage/openlineage.yml").getPath(),
                "openlineage.integration.job.namespace", "reusable-ns",
                "openlineage.integration.job.description", "desc",
                "openlineage.integration.job.tags", "t=v",
                "openlineage.integration.job.owners", "o=w");

        LineageEmitter emitter1 = factory.get(new ConnectorContext("test-connector", "mysql", "0", "3.2.0.Final", config));
        LineageEmitter emitter2 = factory.get(new ConnectorContext("test-connector", "mysql", "0", "3.2.0.Final", config));

        assertSame(
                ((OpenLineageEmitter) emitter1).getContext(),
                ((OpenLineageEmitter) emitter2).getContext());
    }
}
