/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.util.Clock;
import oracle.streams.ChunkColumnValue;
import oracle.streams.DDLLCR;
import oracle.streams.LCR;
import oracle.streams.RowLCR;
import oracle.streams.StreamsException;
import oracle.streams.XStreamLCRCallbackHandler;

/**
 * Handler for Oracle DDL and DML events. Just forwards events to the {@link EventDispatcher}.
 *
 * @author Gunnar Morling
 */
class LcrEventHandler implements XStreamLCRCallbackHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleStreamingChangeEventSource.class);

    private final ErrorHandler errorHandler;
    private final EventDispatcher dispatcher;
    private final Clock clock;
    private final RelationalDatabaseSchema schema;
    private final OracleOffsetContext offsetContext;

    public LcrEventHandler(ErrorHandler errorHandler, EventDispatcher dispatcher, Clock clock, RelationalDatabaseSchema schema, OracleOffsetContext offsetContext) {
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
    }

    @Override
    public void processLCR(LCR lcr) throws StreamsException {
        offsetContext.setPosition(lcr.getPosition());
        offsetContext.setTransactionId(lcr.getTransactionId());
        offsetContext.setSourceTime(lcr.getSourceTime().timestampValue().toInstant());

        try {
            if(lcr instanceof RowLCR) {
                dispatchDataChangeEvent((RowLCR) lcr);
            }
            else if (lcr instanceof DDLLCR) {
                dispatchSchemaChangeEvent((DDLLCR) lcr);
            }
        }
        // nothing to be done here if interrupted; the event loop will be stopped in the streaming source
        catch (InterruptedException e) {
            Thread.interrupted();
            LOGGER.info("Received signal to stop, event loop will halt");
        }
    }

    private void dispatchDataChangeEvent(RowLCR lcr) throws InterruptedException {
        LOGGER.debug("Processing DML event {}", lcr);

        if(RowLCR.COMMIT.equals(lcr.getCommandType())) {
            return;
        }

        TableId tableId = getTableId(lcr);

        dispatcher.dispatchDataChangeEvent(
                offsetContext,
                tableId,
                () -> new OracleChangeRecordEmitter(lcr, schema.getTable(tableId), clock),
                DataChangeEvent::new
        );
    }

    private void dispatchSchemaChangeEvent(DDLLCR ddlLcr) throws InterruptedException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Processing DDL event {}", ddlLcr.getDDLText());
        }

        TableId tableId = getTableId(ddlLcr);

        dispatcher.dispatchSchemaChangeEvent(
                tableId,
                (tid, r) -> {
                    SchemaChangeEventType eventType = getSchemaChangeEventType(ddlLcr);
                    if (eventType != null) {
                        Table table = new OracleDdlParser().parseCreateTable(tid, ddlLcr.getDDLText());
                        r.schemaChangeEvent(new SchemaChangeEvent(ddlLcr.getDDLText(), table, eventType));
                    }
                }
        );
    }

    private SchemaChangeEventType getSchemaChangeEventType(DDLLCR ddlLcr) {
        switch(ddlLcr.getCommandType()) {
            case "CREATE TABLE": return SchemaChangeEventType.CREATE;
            case "ALTER TABLE": throw new UnsupportedOperationException("ALTER TABLE not yet implemented");
            case "DROP TABLE": throw new UnsupportedOperationException("DROP TABLE not yet implemented");
            default: return null;
        }
    }

    private TableId getTableId(LCR lcr) {
        return new TableId(lcr.getSourceDatabaseName(), lcr.getObjectOwner(), lcr.getObjectName());
    }

    @Override
    public void processChunk(ChunkColumnValue arg0) throws StreamsException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public LCR createLCR() throws StreamsException {
        throw new UnsupportedOperationException("Should never be called");
    }

    @Override
    public ChunkColumnValue createChunk() throws StreamsException {
        throw new UnsupportedOperationException("Should never be called");
    }
}