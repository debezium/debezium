/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.metadata;

import java.util.List;

import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.metadata.ComponentMetadataProvider;
import io.debezium.transforms.ByLogicalTableRouter;
import io.debezium.transforms.ExtractChangedRecordState;
import io.debezium.transforms.ExtractNewRecordState;
import io.debezium.transforms.ExtractSchemaToNewRecord;
import io.debezium.transforms.GeometryFormatTransformer;
import io.debezium.transforms.HeaderToValue;
import io.debezium.transforms.SchemaChangeEventFilter;
import io.debezium.transforms.SwapGeometryCoordinates;
import io.debezium.transforms.TimezoneConverter;
import io.debezium.transforms.VectorToJsonConverter;

/**
 * Aggregator for all Debezium transformation metadata.
 */
public class TransformsMetadataProvider implements ComponentMetadataProvider {

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                componentMetadataFactory.createComponentMetadata(new ByLogicalTableRouter<>(), io.debezium.Module.version()),
                componentMetadataFactory.createComponentMetadata(new ExtractChangedRecordState<>(), io.debezium.Module.version()),
                componentMetadataFactory.createComponentMetadata(new ExtractNewRecordState<>(), io.debezium.Module.version()),
                componentMetadataFactory.createComponentMetadata(new ExtractSchemaToNewRecord<>(), io.debezium.Module.version()),
                componentMetadataFactory.createComponentMetadata(new GeometryFormatTransformer<>(), io.debezium.Module.version()),
                componentMetadataFactory.createComponentMetadata(new HeaderToValue<>(), io.debezium.Module.version()),
                componentMetadataFactory.createComponentMetadata(new SchemaChangeEventFilter<>(), io.debezium.Module.version()),
                componentMetadataFactory.createComponentMetadata(new SwapGeometryCoordinates<>(), io.debezium.Module.version()),
                componentMetadataFactory.createComponentMetadata(new TimezoneConverter<>(), io.debezium.Module.version()),
                componentMetadataFactory.createComponentMetadata(new VectorToJsonConverter<>(), io.debezium.Module.version()));
    }

}
