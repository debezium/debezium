/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;

/**
 * {@link SchemaChangeEventEmitter} implementation based on SqlServer.
 *
 * @author Jiri Pechanec
 */
public class SqlServerSchemaChangeEventEmitter implements SchemaChangeEventEmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSchemaChangeEventEmitter.class);

    private final SqlServerOffsetContext offsetContext;
    private final TableId tableId;

    public SqlServerSchemaChangeEventEmitter(SqlServerOffsetContext offsetContext, TableId tableId) {
        this.offsetContext = offsetContext;
        this.tableId = tableId;
    }

    @Override
    public void emitSchemaChangeEvent(Receiver receiver) throws InterruptedException {
//        SchemaChangeEventType eventType = getSchemaChangeEventType();
//        if (eventType == null) {
//            return;
//        }
//
//        Tables tables = new Tables();
//
//        SqlServerDdlParser parser = new SqlServerDdlParser();
//        parser.setCurrentDatabase(ddlLcr.getSourceDatabaseName());
//        parser.setCurrentSchema(ddlLcr.getObjectOwner());
//        parser.parse(ddlLcr.getDDLText(), tables);
//
//        Set<TableId> changedTableIds = tables.drainChanges();
//        if (changedTableIds.isEmpty()) {
//            throw new IllegalArgumentException("Couldn't parse DDL statement " + ddlLcr.getDDLText());
//        }
//
//        Table table = tables.forTable(tableId);
//
//        receiver.schemaChangeEvent(new SchemaChangeEvent(offsetContext.getPartition(), offsetContext.getOffset(), ddlLcr.getSourceDatabaseName(), ddlLcr.getObjectOwner(), ddlLcr.getDDLText(), table, eventType, false));
    }

    private SchemaChangeEventType getSchemaChangeEventType() {
//        switch(ddlLcr.getCommandType()) {
//            case "CREATE TABLE": return SchemaChangeEventType.CREATE;
//            case "ALTER TABLE": LOGGER.warn("ALTER TABLE not yet implemented");
//            case "DROP TABLE": LOGGER.warn("DROP TABLE not yet implemented");
//            default:
//                LOGGER.debug("Ignoring DDL event of type {}", ddlLcr.getCommandType());
//                return null;
//        }
        return null;
    }
}
