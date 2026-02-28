/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.metadata;

import java.util.List;

import io.debezium.connector.oracle.Module;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.converters.NumberOneToBooleanConverter;
import io.debezium.connector.oracle.converters.NumberToZeroScaleConverter;
import io.debezium.connector.oracle.converters.RawToStringConverter;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.metadata.ComponentMetadataProvider;

/**
 * Aggregator for all Oracle connector and custom converter metadata.
 */
public class OracleMetadataProvider implements ComponentMetadataProvider {

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                componentMetadataFactory.createComponentMetadata(new OracleConnector(), Module.version()),
                componentMetadataFactory.createComponentMetadata(new NumberToZeroScaleConverter(), Module.version()),
                componentMetadataFactory.createComponentMetadata(new RawToStringConverter(), Module.version()),
                componentMetadataFactory.createComponentMetadata(new NumberOneToBooleanConverter(), Module.version()));
    }
}
