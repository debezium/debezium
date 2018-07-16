/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

// TODO generify into HistorizedRelationalDatabaseSchema

public class OracleDatabaseSchema extends HistorizedRelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleDatabaseSchema.class);

    private final DatabaseHistory databaseHistory;

    public OracleDatabaseSchema(OracleConnectorConfig connectorConfig, SchemaNameAdjuster schemaNameAdjuster, TopicSelector<TableId> topicSelector, OracleConnection connection) {
        super(connectorConfig, topicSelector, connectorConfig.getTableFilters().dataCollectionFilter(), null,
                new TableSchemaBuilder(new OracleValueConverters(connection), schemaNameAdjuster, SourceInfo.SCHEMA),
                false);
        this.databaseHistory = connectorConfig.getDatabaseHistory();
        this.databaseHistory.start();
    }

    @Override
    public void recover(OffsetContext offset) {
        databaseHistory.recover(offset.getPartition(), offset.getOffset(), tables(), new OracleDdlParser());
        for (TableId tableId : tableIds()) {
            buildAndRegisterSchema(tableFor(tableId));
        }
    }

    @Override
    public void close() {
        databaseHistory.stop();
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChange) {
        LOGGER.debug("Applying schema change event {}", schemaChange);

        // just a single table per DDL event for Oracle
        Table table = schemaChange.getTables().iterator().next();
        buildAndRegisterSchema(table);
        tables().overwriteTable(table);

        TableChanges tableChanges = null;
        if (schemaChange.getType() == SchemaChangeEventType.CREATE && schemaChange.isFromSnapshot()) {
            tableChanges = new TableChanges();
            tableChanges.create(table);
        }

        databaseHistory.record(schemaChange.getPartition(), schemaChange.getOffset(), schemaChange.getDatabase(),
                schemaChange.getSchema(), schemaChange.getDdl(), tableChanges);
    }
}
