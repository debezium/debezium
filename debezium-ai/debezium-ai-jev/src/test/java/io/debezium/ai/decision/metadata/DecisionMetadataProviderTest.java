/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.ai.decision.DecisionToHeaders;
import io.debezium.ai.decision.Module;
import io.debezium.config.Field;

/**
 * Tests of {@link DecisionMetadataProvider}.
 *
 * @author Jiri Pechanec
 */
public class DecisionMetadataProviderTest {

    @Test
    public void shouldDescribeTheDecisionTransformation() {
        final var metadata = new DecisionMetadataProvider().getConnectorMetadata();

        assertThat(metadata).hasSize(1);
        final var descriptor = metadata.get(0).getComponentDescriptor();
        assertThat(descriptor.getClassName()).isEqualTo(DecisionToHeaders.class.getName());
        assertThat(descriptor.getType()).isEqualTo("transformation");
        assertThat(descriptor.getVersion()).isEqualTo(Module.version());
    }

    @Test
    public void shouldExposeGenericAndProviderFields() {
        final var metadata = new DecisionMetadataProvider().getConnectorMetadata().get(0);

        assertThat(metadata.getConfigDefinition().all())
                .extracting(Field::name)
                .contains("decision.provider", "state.fields", "state.format", "questions", "headers.details", "decision.error.mode", "jev.api.key", "jev.base.url");
    }
}
