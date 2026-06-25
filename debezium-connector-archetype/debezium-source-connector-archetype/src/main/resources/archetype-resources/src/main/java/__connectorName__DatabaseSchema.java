/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.util.HashMap;
import java.util.Map;

import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.SchemaNameAdjuster;

/**
 * Maintains the Kafka Connect schemas (key, value, envelope) for each data collection
 * tracked by the ${connectorName} connector.
 *
 * <p>Call {@link #registerSchema} whenever a collection's structure becomes known
 * (typically in the snapshot or when the source signals a schema change). The
 * {@link io.debezium.pipeline.EventDispatcher} calls {@link #schemaFor} to look up the
 * schema before dispatching each event.
 */
public class ${connectorName}DatabaseSchema
        implements DatabaseSchema<TableId> {

    private final Map<String, DataCollectionSchema> schemas = new HashMap<>();
    private final ${connectorName}ConnectorConfig config;
    private final SchemaNameAdjuster schemaNameAdjuster;

    public ${connectorName}DatabaseSchema(${connectorName}ConnectorConfig config,
                                          SchemaNameAdjuster schemaNameAdjuster) {
        this.config = config;
        this.schemaNameAdjuster = schemaNameAdjuster;
    }

    /**
     * Registers (or replaces) the schema for the given collection.
     * Call this whenever the structure of a collection is discovered or changes.
     */
    public void registerSchema(TableId id, DataCollectionSchema schema) {
        schemas.put(id.identifier(), schema);
    }

    @Override
    public DataCollectionSchema schemaFor(TableId id) {
        return schemas.get(id.identifier());
    }

    @Override
    public boolean tableInformationComplete() {
        return true;
    }

    @Override
    public boolean isHistorized() {
        return false;
    }

    @Override
    public void close() {
    }
}
