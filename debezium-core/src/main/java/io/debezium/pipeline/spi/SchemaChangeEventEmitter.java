/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.SchemaChangeEvent;

/**
 * Emits one or more change records - specific to a given {@link DataCollectionSchema}.
 *
 * @author Gunnar Morling
 */
public interface SchemaChangeEventEmitter {

    void emitSchemaChangeEvent(Receiver receiver) throws InterruptedException;

    public interface Receiver {
        void schemaChangeEvent(SchemaChangeEvent event) throws InterruptedException;
    }
}
