/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.metadata;

import java.util.List;

import io.debezium.config.Field;
import io.debezium.metadata.ComponentDescriptor;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;
import io.debezium.metadata.ComponentMetadataUtils;
import io.debezium.transforms.ByLogicalTableRouter;
import io.debezium.transforms.ExtractChangedRecordState;
import io.debezium.transforms.ExtractNewRecordState;
import io.debezium.transforms.ExtractSchemaToNewRecord;
import io.debezium.transforms.GeometryFormatTransformer;
import io.debezium.transforms.HeaderToValue;
import io.debezium.transforms.Module;
import io.debezium.transforms.SchemaChangeEventFilter;
import io.debezium.transforms.SwapGeometryCoordinates;
import io.debezium.transforms.TimezoneConverter;
import io.debezium.transforms.VectorToJsonConverter;

/**
 * Aggregator for all debezium-transforms transformation metadata.
 */
public class TransformsMetadataProvider implements ComponentMetadataProvider {

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                createComponentMetadata(ByLogicalTableRouter.class),
                createComponentMetadata(ExtractChangedRecordState.class),
                createComponentMetadata(ExtractNewRecordState.class),
                createComponentMetadata(ExtractSchemaToNewRecord.class),
                createComponentMetadata(GeometryFormatTransformer.class),
                createComponentMetadata(HeaderToValue.class),
                createComponentMetadata(SchemaChangeEventFilter.class),
                createComponentMetadata(SwapGeometryCoordinates.class),
                createComponentMetadata(TimezoneConverter.class),
                createComponentMetadata(VectorToJsonConverter.class));
    }

    private ComponentMetadata createComponentMetadata(Class<?> componentClass) {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor(componentClass.getName(), Module.version());
            }

            @Override
            public Field.Set getComponentFields() {
                return ComponentMetadataUtils.extractFieldConstants(componentClass);
            }
        };
    }
}
