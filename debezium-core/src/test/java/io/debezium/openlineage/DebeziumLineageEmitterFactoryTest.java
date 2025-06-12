/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
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
        Configuration config = Configuration.create()
                .with("name", "test-connector")
                .with("openlineage.integration.enabled", false)
                .build();

        LineageEmitter emitter = factory.get(new ConnectorContext("test-connector", "mysql", "0", config));

        assertNotNull(emitter);
        assertTrue(emitter instanceof NoOpLineageEmitter);
    }

    @Test
    public void shouldReturnOpenLineageEmitterWhenEnabled() {
        Configuration config = Configuration.create()
                .with("name", "test-connector")
                .with("openlineage.integration.enabled", true)
                .with("openlineage.integration.config.file.path",
                        DebeziumLineageEmitterFactoryTest.class.getClassLoader().getResource("openlineage/openlineage.yml").getPath())
                .with("openlineage.integration.job.namespace", "test-namespace")
                .with("openlineage.integration.job.description", "test-desc")
                .with("openlineage.integration.job.tags", "tag1=value1")
                .with("openlineage.integration.job.owners", "owner1=teamA")
                .build();

        LineageEmitter emitter = factory.get(new ConnectorContext("test-connector", "mysql", "0", config));

        assertNotNull(emitter);
        assertTrue(emitter instanceof OpenLineageEmitter);
    }

    @Test
    public void shouldReuseOpenLineageContextAcrossCalls() {

        Configuration config = Configuration.create()
                .with("name", "test-connector")
                .with("openlineage.integration.enabled", true)
                .with("openlineage.integration.config.file.path",
                        DebeziumLineageEmitterFactoryTest.class.getClassLoader().getResource("openlineage/openlineage.yml").getPath())
                .with("openlineage.integration.job.namespace", "reusable-ns")
                .with("openlineage.integration.job.description", "desc")
                .with("openlineage.integration.job.tags", "t=v")
                .with("openlineage.integration.job.owners", "o=w")
                .build();

        LineageEmitter emitter1 = factory.get(new ConnectorContext("test-connector", "mysql", "0", config));
        LineageEmitter emitter2 = factory.get(new ConnectorContext("test-connector", "mysql", "0", config));

        assertSame(
                ((OpenLineageEmitter) emitter1).getContext(),
                ((OpenLineageEmitter) emitter2).getContext());
    }
}
