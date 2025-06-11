/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.emitter;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.common.ConnectorState;
import io.debezium.openlineage.dataset.DatasetMetadata;

/**
 * A no-operation implementation of {@link LineageEmitter} that performs no actual lineage emission.
 * <p>
 * This implementation is used as a default when lineage emission is disabled. It logs debug messages
 * for all emit operations but does not perform any actual lineage tracking or data emission.
 * Users who want to enable actual lineage emission should set the configuration property
 * {@code openlineage.integration.enabled=true}.
 * </p>
 * <p>
 * All emit methods in this class are effectively no-ops that only log the operations at debug level.
 * </p>
 */
public class NoOpLineageEmitter implements LineageEmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(NoOpLineageEmitter.class);

    public NoOpLineageEmitter() {
        LOGGER.info("You are using a no-op lineage emitter. If you want to emit open lineage event, please set 'openlineage.integration.enabled=true'");
    }

    @Override
    public void emit(ConnectorState state) {
        LOGGER.debug("Emitting lineage event for {}", state.name());
    }

    @Override
    public void emit(ConnectorState state, Throwable t) {
        LOGGER.debug("Emitting lineage event for {}", state.name(), t);
    }

    @Override
    public void emit(ConnectorState state, List<DatasetMetadata> datasetMetadata) {
        LOGGER.debug("Emitting lineage event for {} for dataset {}", state.name(), datasetMetadata);
    }

    @Override
    public void emit(ConnectorState state, List<DatasetMetadata> datasetMetadata, Throwable t) {
        LOGGER.debug("Emitting lineage event for {} for dataset {}", state.name(), datasetMetadata, t);
    }
}
