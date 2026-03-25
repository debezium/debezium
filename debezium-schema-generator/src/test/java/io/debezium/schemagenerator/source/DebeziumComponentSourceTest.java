/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.metadata.ComponentMetadata;

/**
 * Tests for {@link DebeziumComponentSource}.
 */
class DebeziumComponentSourceTest {

    @Test
    void shouldReturnNonNullListWhenDiscovering() {

        DebeziumComponentSource source = new DebeziumComponentSource(null);


        List<ComponentMetadata> components = source.discoverComponents();


        assertThat(components).isNotNull();
    }

    @Test
    void shouldReturnDebeziumComponentsName() {

        DebeziumComponentSource source = new DebeziumComponentSource(null);


        String name = source.getName();


        assertThat(name).isEqualTo("Debezium Components");
    }

    @Test
    void shouldImplementComponentSourceInterface() {

        DebeziumComponentSource source = new DebeziumComponentSource(null);


        assertThat(source).isInstanceOf(ComponentSource.class);
    }
}
