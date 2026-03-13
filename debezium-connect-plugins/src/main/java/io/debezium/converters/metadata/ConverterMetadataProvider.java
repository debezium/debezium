/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters.metadata;

import java.util.List;

import io.debezium.Module;
import io.debezium.converters.BinaryDataConverter;
import io.debezium.converters.ByteArrayConverter;
import io.debezium.converters.CloudEventsConverter;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.metadata.ComponentMetadataProvider;

/**
 * Aggregator for all Debezium converters metadata.
 */
public class ConverterMetadataProvider implements ComponentMetadataProvider {

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                componentMetadataFactory.createComponentMetadata(new BinaryDataConverter(), Module.version()),
                componentMetadataFactory.createComponentMetadata(new ByteArrayConverter(), Module.version()),
                componentMetadataFactory.createComponentMetadata(new CloudEventsConverter(), Module.version()));
    }

}
