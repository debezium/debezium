/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.util.Set;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import oracle.streams.DDLLCR;

/**
 * {@link SchemaChangeEventEmitter} implementation based on Oracle.
 *
 * @author Gunnar Morling
 */
public class OracleSchemaChangeEventEmitter implements SchemaChangeEventEmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSchemaChangeEventEmitter.class);

    private final OracleOffsetContext offsetContext;
    private final TableId tableId;
    private final DDLLCR ddlLcr;

    public OracleSchemaChangeEventEmitter(OracleOffsetContext offsetContext, TableId tableId, DDLLCR ddlLcr) {
        this.offsetContext = offsetContext;
        this.tableId = tableId;
        this.ddlLcr = ddlLcr;
    }

    @Override
    public void emitSchemaChangeEvent(Receiver receiver) throws InterruptedException {
        SchemaChangeEventType eventType = getSchemaChangeEventType();
        if (eventType == null) {
            return;
        }

        Tables tables = new Tables();

        OracleDdlParser parser = new OracleDdlParser();
        parser.setCurrentDatabase(ddlLcr.getSourceDatabaseName());
        parser.setCurrentSchema(ddlLcr.getObjectOwner());
        parser.parse(ddlLcr.getDDLText(), tables);

        Set<TableId> changedTableIds = tables.drainChanges();
        if (changedTableIds.isEmpty()) {
            throw new IllegalArgumentException("Couldn't parse DDL statement " + ddlLcr.getDDLText());
        }

        Table table = tables.forTable(tableId);

        receiver.schemaChangeEvent(new SchemaChangeEvent(offsetContext.getPartition(), offsetContext.getOffset(), ddlLcr.getSourceDatabaseName(), ddlLcr.getObjectOwner(), ddlLcr.getDDLText(), table, eventType, false));
    }

    private SchemaChangeEventType getSchemaChangeEventType() {
        switch(ddlLcr.getCommandType()) {
            case "CREATE TABLE":
                return SchemaChangeEventType.CREATE;
            case "ALTER TABLE":
                LOGGER.warn("ALTER TABLE not yet implemented");
                break;
            case "DROP TABLE":
                LOGGER.warn("DROP TABLE not yet implemented");
                break;
            default:
                LOGGER.debug("Ignoring DDL event of type {}", ddlLcr.getCommandType());
        }

        return null;
    }
}
