/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.common.BaseSourceTask;
import io.debezium.relational.Table;

public class NoOpLineageEmitter implements LineageEmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(NoOpLineageEmitter.class);

    public NoOpLineageEmitter() {
        LOGGER.info("You are using a no-op lineage emitter. If you want to emit open lineage event, please set 'openlineage.integration.enabled=true'");
    }

    @Override
    public void emit(BaseSourceTask.State state) {
        LOGGER.debug("Emitting lineage event for {}", state.name());
    }

    @Override
    public void emit(BaseSourceTask.State state, Throwable t) {
        LOGGER.debug("Emitting lineage event for {}", state.name(), t);
    }

    @Override
    public void emit(BaseSourceTask.State state, Table event) {
        LOGGER.debug("Emitting lineage event for {} for dataset {}", state.name(), event);
    }

    @Override
    public void emit(BaseSourceTask.State state, Table event, Throwable t) {
        LOGGER.debug("Emitting lineage event for {} for dataset {}", state.name(), event, t);
    }
}
