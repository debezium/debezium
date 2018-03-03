/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import io.debezium.pipeline.spi.OffsetContext;

public interface DatabaseSchema {

    void applySchemaChange(SchemaChangeEvent schemaChange);

    void recover(OffsetContext offset);

    void close();

    DataCollectionSchema getDataCollectionSchema(DataCollectionId id);
}
