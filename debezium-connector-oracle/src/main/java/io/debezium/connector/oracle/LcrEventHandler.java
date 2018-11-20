/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import oracle.streams.ChunkColumnValue;
import oracle.streams.DDLLCR;
import oracle.streams.LCR;
import oracle.streams.RowLCR;
import oracle.streams.StreamsException;
import oracle.streams.XStreamLCRCallbackHandler;
import oracle.streams.XStreamUtility;

/**
 * Handler for Oracle DDL and DML events. Just forwards events to the {@link EventDispatcher}.
 *
 * @author Gunnar Morling
 */
class LcrEventHandler implements XStreamLCRCallbackHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleStreamingChangeEventSource.class);

    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final RelationalDatabaseSchema schema;
    private final OracleOffsetContext offsetContext;
    private final boolean tablenameCaseInsensitive;

    public LcrEventHandler(ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock, RelationalDatabaseSchema schema, OracleOffsetContext offsetContext,boolean tablenameCaseInsensitive) {
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.tablenameCaseInsensitive = tablenameCaseInsensitive;
    }

    @Override
    public void processLCR(LCR lcr) throws StreamsException {
        long scn = convertPositionToScn(lcr.getPosition());

        // After a restart it may happen we get the event with the last processed SCN again
        if (scn <= offsetContext.getScn()) {
            LOGGER.debug("Ignoring change event with already processed SCN {}", scn);
            return;
        }

        offsetContext.setScn(scn);
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
        // XStream's receiveLCRCallback() doesn't reliably propagate exceptions, so we do that ourselves here
        catch (Exception e) {
            errorHandler.setProducerThrowable(e);
        }
    }

    private long convertPositionToScn(byte[] position) {
        try {
            return XStreamUtility.getSCNFromPosition(position).longValue();
        }
        catch (SQLException | StreamsException e) {
            throw new RuntimeException(e);
        }
    }

    private void dispatchDataChangeEvent(RowLCR lcr) throws InterruptedException {
        LOGGER.debug("Processing DML event {}", lcr);

        if(RowLCR.COMMIT.equals(lcr.getCommandType())) {
            return;
        }

        TableId tableId = getTableId(lcr);

        dispatcher.dispatchDataChangeEvent(
                tableId,
                new XStreamChangeRecordEmitter(offsetContext, lcr, schema.tableFor(tableId), clock)
        );
    }

    private void dispatchSchemaChangeEvent(DDLLCR ddlLcr) throws InterruptedException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Processing DDL event {}", ddlLcr.getDDLText());
        }

        TableId tableId = getTableId(ddlLcr);

        dispatcher.dispatchSchemaChangeEvent(
                tableId,
                new OracleSchemaChangeEventEmitter(offsetContext, tableId, ddlLcr)
        );
    }

    private TableId getTableId(LCR lcr) {
        if (!this.tablenameCaseInsensitive) {
            return new TableId(lcr.getSourceDatabaseName(), lcr.getObjectOwner(), lcr.getObjectName());
        }
        else {
            return new TableId(lcr.getSourceDatabaseName().toLowerCase(), lcr.getObjectOwner(), lcr.getObjectName().toLowerCase());
        }
        
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