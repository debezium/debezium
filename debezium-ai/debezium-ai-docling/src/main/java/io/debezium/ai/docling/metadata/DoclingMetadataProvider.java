/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.docling.metadata;

import java.util.List;

import io.debezium.ai.docling.FieldToDocling;
import io.debezium.ai.docling.Module;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.metadata.ComponentMetadataProvider;

/**
 * Aggregator for all Docling SMT metadata.
 */
public class DoclingMetadataProvider implements ComponentMetadataProvider {
    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                componentMetadataFactory.createComponentMetadata(new FieldToDocling<>(), Module.version()));
    }
}
