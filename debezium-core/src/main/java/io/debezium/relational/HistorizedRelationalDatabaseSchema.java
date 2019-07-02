/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.HistorizedDatabaseSchema;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;

/**
 * A {@link DatabaseSchema} or a relational database which has a schema history, that can be recovered to the current
 * state when restarting a connector.
 *
 * @author Gunnar Morling
 *
 */
public abstract class HistorizedRelationalDatabaseSchema extends RelationalDatabaseSchema
        implements HistorizedDatabaseSchema<TableId> {

    private final DatabaseHistory databaseHistory;

    protected HistorizedRelationalDatabaseSchema(HistorizedRelationalDatabaseConnectorConfig config, TopicSelector<TableId> topicSelector,
            TableFilter tableFilter, ColumnNameFilter columnFilter, TableSchemaBuilder schemaBuilder,
            boolean tableIdCaseInsensitive) {
        super(config, topicSelector, tableFilter, columnFilter, schemaBuilder, tableIdCaseInsensitive);

        this.databaseHistory = config.getDatabaseHistory();
        this.databaseHistory.start();
    }

    @Override
    public void recover(OffsetContext offset) {
        databaseHistory.recover(offset.getPartition(), offset.getOffset(), tables(), getDdlParser());
        for (TableId tableId : tableIds()) {
            buildAndRegisterSchema(tableFor(tableId));
        }
    }

    @Override
    public void close() {
        databaseHistory.stop();
    }

    /**
     * Configures a storage used to store history, e.g. in Kafka case it creates topic with
     * required parameters.
     */
    @Override
    public void initializeStorage() {
        if (!databaseHistory.exists()) {
            databaseHistory.initializeStorage();
        }
    }

    /**
     * Returns a new instance of the {@link DdlParser} to be used when recovering the schema from a previously persisted
     * history.
     */
    protected abstract DdlParser getDdlParser();

    /**
     * Records the given schema change event in the persistent history.
     *
     * @param schemaChange
     *            The schema change, must not be {@code null}
     * @param tableChanges
     *            A logical representation of the change, may be {@code null} if a specific implementation solely relies
     *            on storing DDL statements in the history
     */
    protected void record(SchemaChangeEvent schemaChange, TableChanges tableChanges) {
        databaseHistory.record(schemaChange.getPartition(), schemaChange.getOffset(), schemaChange.getDatabase(),
                schemaChange.getSchema(), schemaChange.getDdl(), tableChanges);
    }
}
