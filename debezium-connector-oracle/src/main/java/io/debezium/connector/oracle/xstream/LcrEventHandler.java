/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnection.NonRelationalTableException;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleSchemaChangeEventEmitter;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.xstream.XstreamStreamingChangeEventSource.PositionAndScn;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

import oracle.streams.ChunkColumnValue;
import oracle.streams.DDLLCR;
import oracle.streams.DefaultRowLCR;
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

    private final OracleConnectorConfig connectorConfig;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<OraclePartition, TableId> dispatcher;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final OraclePartition partition;
    private final OracleOffsetContext offsetContext;
    private final boolean tablenameCaseInsensitive;
    private final XstreamStreamingChangeEventSource eventSource;
    private final XStreamStreamingChangeEventSourceMetrics streamingMetrics;
    private final Map<String, ChunkColumnValues> columnChunks;
    private RowLCR currentRow;

    LcrEventHandler(OracleConnectorConfig connectorConfig, ErrorHandler errorHandler,
                    EventDispatcher<OraclePartition, TableId> dispatcher, Clock clock,
                    OracleDatabaseSchema schema, OraclePartition partition, OracleOffsetContext offsetContext,
                    boolean tablenameCaseInsensitive, XstreamStreamingChangeEventSource eventSource,
                    XStreamStreamingChangeEventSourceMetrics streamingMetrics) {
        this.connectorConfig = connectorConfig;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.tablenameCaseInsensitive = tablenameCaseInsensitive;
        this.eventSource = eventSource;
        this.streamingMetrics = streamingMetrics;
        this.columnChunks = new LinkedHashMap<>();
    }

    @Override
    public void processLCR(LCR lcr) throws StreamsException {
        LOGGER.trace("Received LCR {}", lcr);
        try {
            // First set watermark to flush messages seen
            setWatermark();
            columnChunks.clear();

            final LcrPosition lcrPosition = new LcrPosition(lcr.getPosition());

            // After a restart it may happen we get the event with the last processed LCR again
            LcrPosition offsetLcrPosition = LcrPosition.valueOf(offsetContext.getLcrPosition());
            if (lcrPosition.compareTo(offsetLcrPosition) <= 0) {
                if (LOGGER.isDebugEnabled()) {
                    final LcrPosition recPosition = offsetLcrPosition;
                    LOGGER.debug("Ignoring change event with already processed SCN/LCR Position {}/{}, last recorded {}/{}",
                            lcrPosition,
                            lcrPosition.getScn(),
                            recPosition != null ? recPosition : "none",
                            recPosition != null ? recPosition.getScn() : "none");
                }
                return;
            }

            offsetContext.setRowId(""); // specifically reset on each event
            offsetContext.setScn(lcrPosition.getScn());
            offsetContext.setEventScn(lcrPosition.getScn());
            offsetContext.setLcrPosition(lcrPosition.toString());
            offsetContext.setTransactionId(lcr.getTransactionId());
            offsetContext.tableEvent(new TableId(lcr.getSourceDatabaseName(), lcr.getObjectOwner(), lcr.getObjectName()),
                    lcr.getSourceTime().timestampValue().toInstant());

            if (lcr instanceof RowLCR) {
                processRowLCR((RowLCR) lcr);
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

    private void processRowLCR(RowLCR row) throws InterruptedException {
        if (row.getCommandType().equals(RowLCR.LOB_ERASE)) {
            LOGGER.warn("LOB_ERASE for table '{}' is not supported, "
                    + "use DML operations to manipulate LOB columns only.", row.getObjectName());
            return;
        }

        if (row.hasChunkData()) {
            // If the row has chunk data, the RowLCR cannot be immediately dispatched.
            // The handler needs to cache the current row and wait for the chunks to be delivered before
            // the event can be safely dispatched. See processChunk below.
            currentRow = row;
        }
        else {
            // Since the row has no chunk data, it can be dispatched immediately.
            dispatchDataChangeEvent(row, null);
        }
    }

    private void dispatchDataChangeEvent(RowLCR lcr, Map<String, Object> chunkValues) throws InterruptedException {
        LOGGER.info("Processing DML event {}", lcr);

        if (RowLCR.COMMIT.equals(lcr.getCommandType())) {
            final Instant commitTimestamp = lcr.getSourceTime().timestampValue().toInstant();
            dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext, commitTimestamp);
            return;
        }

        TableId tableId = getTableId(lcr);

        Table table = schema.tableFor(tableId);
        if (table == null) {
            if (!connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                LOGGER.trace("Table {} is new but excluded, schema change skipped.", tableId);
                return;
            }

            LOGGER.warn("Obtaining schema for table {}, which should be already loaded, this may signal potential bug in fetching table schemas.", tableId);
            final String tableDdl;
            try {
                tableDdl = getTableMetadataDdl(tableId);
            }
            catch (NonRelationalTableException e) {
                LOGGER.warn("Table {} is not a relational table and will be skipped.", tableId);
                streamingMetrics.incrementWarningCount();
                return;
            }

            LOGGER.info("Table {} will be captured.", tableId);
            dispatcher.dispatchSchemaChangeEvent(
                    partition,
                    offsetContext,
                    tableId,
                    new OracleSchemaChangeEventEmitter(
                            connectorConfig,
                            partition,
                            offsetContext,
                            tableId,
                            tableId.catalog(),
                            tableId.schema(),
                            tableDdl,
                            schema,
                            Instant.now(),
                            streamingMetrics,
                            null));

            table = schema.tableFor(tableId);
            if (table == null) {
                return;
            }
        }

        // Xstream does not provide any before state for LOB columns and so this map will be
        // populated here by column name with the OracleValueConverters.UNAVAILABLE_VALUE.
        Map<String, Object> oldChunkValues = new HashMap<>(0);

        if (chunkValues == null) {
            // Happens when dispatching an LCR without any chunk data.
            chunkValues = new HashMap<>(0);
        }

        // LCR events may arrive both with and without chunk data.
        //
        // For example a DELETE by a primary key on a table with LOB columns will not supply any
        // LOB chunk data. In other scenarios such as an UPDATE where a LOB column is modified,
        // the updated LOB value will be provided but the prior value will not be.
        //
        // So in either case, the values need to be serialized here such that any LOB column that
        // is not explicitly provided in the map is initialized with the unavailable value
        // marker object so its transformed correctly by the value converters.

        for (Column column : schema.getLobColumnsForTable(table.id())) {
            // again Xstream doesn't supply before state for LOB values; explicitly use unavailable value
            oldChunkValues.put(column.name(), OracleValueConverters.UNAVAILABLE_VALUE);
            if (!chunkValues.containsKey(column.name())) {
                // Column not supplied, initialize with unavailable value marker
                LOGGER.trace("\tColumn '{}' not supplied, initialized with unavailable value", column.name());
                chunkValues.put(column.name(), OracleValueConverters.UNAVAILABLE_VALUE);
            }
        }

        final Object rowIdObject = lcr.getAttribute("ROW_ID");
        if (rowIdObject != null) {
            offsetContext.setRowId(rowIdObject.toString());
        }

        dispatcher.dispatchDataChangeEvent(
                partition,
                tableId,
                new XStreamChangeRecordEmitter(
                        connectorConfig,
                        partition,
                        offsetContext,
                        lcr,
                        oldChunkValues,
                        chunkValues,
                        schema.tableFor(tableId),
                        schema,
                        clock));
    }

    private void dispatchSchemaChangeEvent(DDLLCR ddlLcr) throws InterruptedException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Processing DDL event {}", ddlLcr.getDDLText());
        }

        TableId tableId = getTableId(ddlLcr);

        dispatcher.dispatchSchemaChangeEvent(
                partition,
                offsetContext,
                tableId,
                new OracleSchemaChangeEventEmitter(
                        connectorConfig,
                        partition,
                        offsetContext,
                        tableId,
                        ddlLcr.getSourceDatabaseName(),
                        ddlLcr.getObjectOwner(),
                        ddlLcr.getDDLText(),
                        schema,
                        ddlLcr.getSourceTime().timestampValue().toInstant(),
                        streamingMetrics,
                        () -> processTruncateEvent(ddlLcr)));
    }

    private void processTruncateEvent(DDLLCR ddlLcr) {
        LOGGER.debug("Handling truncate event");
        DefaultRowLCR rowLCR = new DefaultRowLCR(
                ddlLcr.getSourceDatabaseName(),
                ddlLcr.getCommandType(),
                ddlLcr.getObjectOwner(),
                ddlLcr.getObjectName(),
                ddlLcr.getTransactionId(),
                ddlLcr.getTag(),
                ddlLcr.getPosition(),
                ddlLcr.getSourceTime());
        try {
            dispatchDataChangeEvent(rowLCR, null);
        }
        catch (InterruptedException e) {
            throw new RuntimeException("Interrupted", e);
        }

    }

    private TableId getTableId(LCR lcr) {
        if (!this.tablenameCaseInsensitive) {
            return new TableId(lcr.getSourceDatabaseName(), lcr.getObjectOwner(), lcr.getObjectName());
        }
        else {
            return new TableId(lcr.getSourceDatabaseName(), lcr.getObjectOwner(), lcr.getObjectName().toLowerCase());
        }
    }

    private String getTableMetadataDdl(TableId tableId) throws NonRelationalTableException {
        LOGGER.info("Getting database metadata for table '{}'", tableId);
        final String pdbName = connectorConfig.getPdbName();
        // A separate connection must be used for this out-of-bands query while processing the Xstream callback.
        // This should have negligible overhead as this should happen rarely.
        try (OracleConnection connection = new OracleConnection(connectorConfig.getJdbcConfig(), false)) {
            if (!Strings.isNullOrBlank(pdbName)) {
                connection.setSessionToPdb(pdbName);
            }
            connection.setAutoCommit(false);
            return connection.getTableMetadataDdl(tableId);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to get table DDL metadata for: " + tableId, e);
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
    public void processChunk(ChunkColumnValue chunk) throws StreamsException {
        columnChunks.computeIfAbsent(chunk.getColumnName(), v -> new ChunkColumnValues()).add(chunk);
        if (chunk.isEndOfRow()) {
            resolveAndDispatchCurrentChunkedRow();
        }
    }

    @Override
    public LCR createLCR() throws StreamsException {
        throw new UnsupportedOperationException("Should never be called");
    }

    @Override
    public ChunkColumnValue createChunk() throws StreamsException {
        throw new UnsupportedOperationException("Should never be called");
    }

    private void resolveAndDispatchCurrentChunkedRow() {
        try {
            // Map of resolved chunk values
            Map<String, Object> resolvedChunkValues = new HashMap<>();

            // All chunks have been dispatched to the event handler, combine the chunks now.
            for (Map.Entry<String, ChunkColumnValues> entry : columnChunks.entrySet()) {
                final String columnName = entry.getKey();
                final ChunkColumnValues chunkValues = entry.getValue();

                if (chunkValues.isEmpty()) {
                    LOGGER.trace("Column '{}' has no chunk values.", columnName);
                    continue;
                }

                final int type = chunkValues.getChunkType();
                switch (type) {
                    case ChunkColumnValue.CLOB:
                    case ChunkColumnValue.NCLOB:
                        resolvedChunkValues.put(columnName, chunkValues.getStringValue());
                        break;

                    case ChunkColumnValue.XMLTYPE:
                        resolvedChunkValues.put(columnName, chunkValues.getXmlValue());
                        break;

                    case ChunkColumnValue.RAW:
                    case ChunkColumnValue.BLOB:
                        resolvedChunkValues.put(columnName, chunkValues.getByteArray());
                        break;

                    default:
                        LOGGER.trace("Received an unsupported chunk type '{}' for column '{}', ignored.", type, columnName);
                        break;
                }
            }

            columnChunks.clear();
            dispatchDataChangeEvent(currentRow, resolvedChunkValues);
        }
        catch (InterruptedException e) {
            Thread.interrupted();
            LOGGER.info("Received signal to stop, event loop will halt");
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to process chunk data", e);
        }
    }
}
