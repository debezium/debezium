/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.xstream.XstreamStreamingChangeEventSource.PositionAndScn;
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
import oracle.streams.XStreamOut;

/**
 * Handler for Oracle DDL and DML events. Just forwards events to the {@link EventDispatcher}.
 *
 * @author Gunnar Morling
 */
class LcrEventHandler implements XStreamLCRCallbackHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LcrEventHandler.class);

    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final RelationalDatabaseSchema schema;
    private final OracleOffsetContext offsetContext;
    private final boolean tablenameCaseInsensitive;
    private final XstreamStreamingChangeEventSource eventSource;

    public LcrEventHandler(ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock, RelationalDatabaseSchema schema,
                           OracleOffsetContext offsetContext, boolean tablenameCaseInsensitive, XstreamStreamingChangeEventSource eventSource) {
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.tablenameCaseInsensitive = tablenameCaseInsensitive;
        this.eventSource = eventSource;
    }

    @Override
    public void processLCR(LCR lcr) throws StreamsException {
        LOGGER.trace("Received LCR {}", lcr);

        // First set watermark to flush messages seen
        setWatermark();

        final LcrPosition lcrPosition = new LcrPosition(lcr.getPosition());

        // After a restart it may happen we get the event with the last processed LCR again
        if (lcrPosition.compareTo(offsetContext.getLcrPosition()) <= 0) {
            if (LOGGER.isDebugEnabled()) {
                final LcrPosition recPosition = offsetContext.getLcrPosition();
                LOGGER.debug("Ignoring change event with already processed SCN/LCR Position {}/{}, last recorded {}/{}",
                        lcrPosition,
                        lcrPosition.getScn(),
                        recPosition != null ? recPosition : "none",
                        recPosition != null ? recPosition.getScn() : "none");
            }
            return;
        }

        offsetContext.setScn(lcrPosition.getScn());
        offsetContext.setLcrPosition(lcrPosition);
        offsetContext.setTransactionId(lcr.getTransactionId());
        offsetContext.setSourceTime(lcr.getSourceTime().timestampValue().toInstant());
        offsetContext.setTableId(new TableId(lcr.getSourceDatabaseName(), lcr.getObjectOwner(), lcr.getObjectName()));

        try {
            if (lcr instanceof RowLCR) {
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

    private void dispatchDataChangeEvent(RowLCR lcr) throws InterruptedException {
        LOGGER.debug("Processing DML event {}", lcr);

        if (RowLCR.COMMIT.equals(lcr.getCommandType())) {
            dispatcher.dispatchTransactionCommittedEvent(offsetContext);
            return;
        }

        TableId tableId = getTableId(lcr);

        dispatcher.dispatchDataChangeEvent(
                tableId,
                new XStreamChangeRecordEmitter(offsetContext, lcr, schema.tableFor(tableId), clock));
    }

    private void dispatchSchemaChangeEvent(DDLLCR ddlLcr) throws InterruptedException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Processing DDL event {}", ddlLcr.getDDLText());
        }

        TableId tableId = getTableId(ddlLcr);

        dispatcher.dispatchSchemaChangeEvent(
                tableId,
                new XStreamSchemaChangeEventEmitter(offsetContext, tableId, ddlLcr));
    }

    private TableId getTableId(LCR lcr) {
        if (!this.tablenameCaseInsensitive) {
            return new TableId(lcr.getSourceDatabaseName(), lcr.getObjectOwner(), lcr.getObjectName());
        }
        else {
            return new TableId(lcr.getSourceDatabaseName().toLowerCase(), lcr.getObjectOwner(), lcr.getObjectName().toLowerCase());
        }
    }

    private void setWatermark() {
        if (eventSource.getXsOut() == null) {
            return;
        }
        try {
            final PositionAndScn message = eventSource.receivePublishedPosition();
            if (message == null) {
                return;
            }
            LOGGER.debug("Recording offsets to Oracle");
            if (message.position != null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Recording position {}", message.position);
                }
                eventSource.getXsOut().setProcessedLowWatermark(
                        message.position.getRawPosition(),
                        XStreamOut.DEFAULT_MODE);
            }
            else if (message.scn != null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Recording position with SCN {}", message.scn);
                }
                eventSource.getXsOut().setProcessedLowWatermark(
                        message.scn,
                        XStreamOut.DEFAULT_MODE);
            }
            else {
                LOGGER.warn("Nothing in offsets could be recorded to Oracle");
                return;
            }
            LOGGER.trace("Offsets recorded to Oracle");
        }
        catch (StreamsException e) {
            throw new DebeziumException("Couldn't set processed low watermark", e);
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
