/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.time.Instant;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;

/**
 * Carries the {@code source} metadata block included in every change event.
 *
 * <p>Add connector-specific fields here (e.g. file name, position, sequence number).
 * Exposed fields must also be registered in {@link ${connectorName}SourceInfoStructMaker}.
 */
public class ${connectorName}SourceInfo extends BaseSourceInfo {

    private final CommonConnectorConfig config;

    private Instant timestamp;
    private String schemaName = "";
    private String tableName = "";

    public ${connectorName}SourceInfo(CommonConnectorConfig config) {
        super(config);
        this.config = config;
    }

    /**
     * Records the table and time of the event about to be emitted; called through
     * {@link ${connectorName}OffsetContext#event}.
     */
    void update(Instant timestamp, TableId tableId) {
        this.timestamp = timestamp;
        if (tableId != null) {
            this.schemaName = tableId.schema() != null ? tableId.schema() : "";
            this.tableName = tableId.table() != null ? tableId.table() : "";
        }
    }

    @Override
    protected Instant timestamp() {
        // TODO: this is the time the connector processed the event, passed in by the framework. Replace it
        // with the time the change was committed in the source database; it becomes the ts_ms field of the
        // event's source block.
        return timestamp;
    }

    String schemaName() {
        return schemaName;
    }

    String tableName() {
        return tableName;
    }

    @Override
    protected String database() {
        return config.getLogicalName();
    }
}
