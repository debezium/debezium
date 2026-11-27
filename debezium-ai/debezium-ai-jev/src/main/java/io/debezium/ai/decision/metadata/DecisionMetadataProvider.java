/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.metadata;

import java.util.List;

import io.debezium.ai.decision.DecisionToHeaders;
import io.debezium.ai.decision.Module;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.metadata.ComponentMetadataProvider;

/**
 * Aggregator for all AI decision SMT metadata.
 *
 * @author Jiri Pechanec
 */
public class DecisionMetadataProvider implements ComponentMetadataProvider {
    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                componentMetadataFactory.createComponentMetadata(new DecisionToHeaders<>(), Module.version()));
    }
}
