/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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
import oracle.streams.ColumnValue;
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
    private final OracleConnection jdbcConnection;
    private final Map<String, ChunkColumnValues> columnChunks;
    private RowLCR currentRow;

    LcrEventHandler(OracleConnectorConfig connectorConfig, ErrorHandler errorHandler,
                    EventDispatcher<OraclePartition, TableId> dispatcher, Clock clock,
                    OracleDatabaseSchema schema, OraclePartition partition, OracleOffsetContext offsetContext,
                    boolean tablenameCaseInsensitive, XstreamStreamingChangeEventSource eventSource,
                    XStreamStreamingChangeEventSourceMetrics streamingMetrics,
                    OracleConnection jdbcConnection) {
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
        this.jdbcConnection = jdbcConnection;
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
            offsetContext.setEventCommitScn(lcrPosition.getCommitScn());
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
        LOGGER.debug("Processing DML event {}", lcr);

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
                LOGGER.warn("{} The event will be skipped.", e.getMessage());
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

        Map<String, Object> oldChunkValues = new HashMap<>(0);

        if (chunkValues == null) {
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
        // Note: For LOB_WRITE/LOB_TRIM/LOB_ERASE events, reselectLobValues() replaces
        // the unavailable markers with actual LOB values read from the database.

        for (Column column : schema.getLobColumnsForTable(table.id())) {
            // Xstream doesn't supply before state for LOB values; explicitly use unavailable value
            oldChunkValues.put(column.name(), OracleValueConverters.UNAVAILABLE_VALUE);
            if (!chunkValues.containsKey(column.name())) {
                // Column not supplied, initialize with unavailable value marker
                LOGGER.trace("\tColumn '{}' not supplied, initialized with unavailable value", column.name());
                chunkValues.put(column.name(), OracleValueConverters.UNAVAILABLE_VALUE);
            }
        }

        // For LOB_WRITE/LOB_TRIM/LOB_ERASE, the chunk stream (if any) only carries the partial
        // delta written by DBMS_LOB, not the full post-operation value. Reselect against the
        // current row so downstream consumers see a consistent full LOB state. Gated on these
        // specific command types so regular DML is not slowed down — callers who want
        // per-DML reselection continue to use ReselectColumnsPostProcessor.
        if (RowLCR.LOB_WRITE.equals(lcr.getCommandType())
                || RowLCR.LOB_TRIM.equals(lcr.getCommandType())
                || RowLCR.LOB_ERASE.equals(lcr.getCommandType())) {
            reselectLobValues(lcr, table, chunkValues);
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
        try (OracleConnection connection = new OracleConnection(connectorConfig, false)) {
            if (!Strings.isNullOrBlank(pdbName)) {
                connection.setSessionToPdb(pdbName);
            }
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

    /**
     * Re-reads full LOB column values from the source database for
     * {@code LOB_WRITE} / {@code LOB_TRIM} / {@code LOB_ERASE} events and
     * overwrites the corresponding entries in {@code chunkValues}.
     *
     * <p>The chunk data XStream delivers for these operations is only the
     * partial delta passed to {@code DBMS_LOB.WRITE} / {@code WRITEAPPEND}
     * / {@code TRIM} / {@code ERASE}, not the full post-operation LOB
     * value. Reselecting against the current row is the only way to
     * materialize a consistent full value for downstream consumers.
     *
     * <p>Delegates to {@link OracleConnection#reselectColumns} so this path
     * shares the existing SQL builder, identifier quoting, and flashback
     * fallback logic with the {@code ReselectColumnsPostProcessor} path.
     */
    private void reselectLobValues(RowLCR row, Table table, Map<String, Object> chunkValues) {
        if (chunkValues.isEmpty() || jdbcConnection == null) {
            return;
        }

        final List<String> pkColumns = table.primaryKeyColumnNames();
        if (pkColumns.isEmpty()) {
            LOGGER.warn("Cannot reselect LOB values for table {}: no primary key defined "
                    + "(tx={}, scn={}, rowId={}).",
                    table.id(), row.getTransactionId(), offsetContext.getScn(),
                    row.getAttribute("ROW_ID"));
            return;
        }

        // PK values: prefer newValues, fall back to oldValues for any that are missing.
        final Map<String, Object> pkValuesMap = new LinkedHashMap<>();
        if (row.getNewValues() != null) {
            for (ColumnValue cv : row.getNewValues()) {
                if (pkColumns.contains(cv.getColumnName())) {
                    pkValuesMap.put(cv.getColumnName(), cv.getColumnData());
                }
            }
        }
        if (pkValuesMap.size() < pkColumns.size() && row.getOldValues() != null) {
            for (ColumnValue cv : row.getOldValues()) {
                if (pkColumns.contains(cv.getColumnName())
                        && !pkValuesMap.containsKey(cv.getColumnName())) {
                    pkValuesMap.put(cv.getColumnName(), cv.getColumnData());
                }
            }
        }
        if (pkValuesMap.size() < pkColumns.size()) {
            LOGGER.warn("Cannot reselect LOB values for table {}: incomplete primary key in LCR "
                    + "(tx={}, scn={}, rowId={}).",
                    table.id(), row.getTransactionId(), offsetContext.getScn(),
                    row.getAttribute("ROW_ID"));
            return;
        }

        final List<Object> pkValues = new ArrayList<>(pkColumns.size());
        for (String pkColumn : pkColumns) {
            pkValues.add(pkValuesMap.get(pkColumn));
        }

        final List<String> lobColumnNames = new ArrayList<>(chunkValues.keySet());

        try {
            // reselectColumns advances the ResultSet cursor internally before invoking
            // the consumer, so we read column values directly without calling rs.next().
            // A return of `false` means no matching row was found — typically the row
            // was deleted between the LCR emission and our reselect.
            final boolean found = jdbcConnection.reselectColumns(table, lobColumnNames, pkColumns, pkValues, null, rs -> {
                for (String colName : lobColumnNames) {
                    final Column column = table.columnWithName(colName);
                    if (column == null) {
                        continue;
                    }
                    if (column.jdbcType() == java.sql.Types.BLOB) {
                        final byte[] blobVal = rs.getBytes(colName);
                        if (blobVal != null) {
                            chunkValues.put(colName, blobVal);
                        }
                    }
                    else {
                        final String strVal = rs.getString(colName);
                        if (strVal != null) {
                            chunkValues.put(colName, strVal);
                        }
                    }
                }
            });
            if (!found) {
                LOGGER.warn("Reselect for table {} returned no rows — the row may have been deleted "
                        + "between the LCR and the reselect (tx={}, scn={}, rowId={}).",
                        table.id(), row.getTransactionId(), offsetContext.getScn(),
                        row.getAttribute("ROW_ID"));
            }
        }
        catch (SQLException e) {
            LOGGER.warn("Failed to reselect LOB values for table {} (tx={}, scn={}, rowId={}): {}. "
                    + "Emitting partial chunk data.",
                    table.id(), row.getTransactionId(), offsetContext.getScn(),
                    row.getAttribute("ROW_ID"), e.getMessage());
        }
    }

}
