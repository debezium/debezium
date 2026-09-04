/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection.NonRelationalTableException;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleSchemaChangeEventEmitter;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.connector.oracle.jdbc.OracleConnectionFactory;
import io.debezium.connector.oracle.olr.client.OlrNetworkClient;
import io.debezium.connector.oracle.olr.client.PayloadEvent;
import io.debezium.connector.oracle.olr.client.PayloadEvent.Type;
import io.debezium.connector.oracle.olr.client.StreamingEvent;
import io.debezium.connector.oracle.olr.client.payloads.AbstractMutationEvent;
import io.debezium.connector.oracle.olr.client.payloads.PayloadSchema;
import io.debezium.connector.oracle.olr.client.payloads.SchemaChangeEvent;
import io.debezium.connector.oracle.olr.client.payloads.Values;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

import oracle.sql.RAW;

/**
 * An implementation of {@link StreamingChangeEventSource} based on OpenLogReplicator.
 *
 * @author Chris Cranford
 */
public class OpenLogReplicatorStreamingChangeEventSource implements StreamingChangeEventSource<OraclePartition, OracleOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenLogReplicatorStreamingChangeEventSource.class);

    private final OracleConnectorConfig connectorConfig;
    private final OracleConnectionFactory connectionFactory;
    private final EventDispatcher<OraclePartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final OpenLogReplicatorStreamingChangeEventSourceMetrics streamingMetrics;
    private final SnapshotterService snapshotterService;

    private OlrNetworkClient client;
    private OraclePartition partition;
    private OracleOffsetContext offsetContext;
    private boolean transactionEvents = false;
    /**
     * The position the connector had emitted up to when it last stopped.
     *
     * <p>Streaming resumes at the start of the transaction that position falls in, so the changes
     * before it arrive a second time and are discarded here rather than being filtered out by the
     * server. Cleared once the stream advances past that transaction.
     */
    private Scn replayScn;
    private Long replayScnIndex;
    private String replayTransactionId;

    public OpenLogReplicatorStreamingChangeEventSource(OracleConnectorConfig connectorConfig, OracleConnectionFactory connectionFactory,
                                                       EventDispatcher<OraclePartition, TableId> dispatcher,
                                                       ErrorHandler errorHandler, Clock clock,
                                                       OracleDatabaseSchema schema,
                                                       OpenLogReplicatorStreamingChangeEventSourceMetrics streamingMetrics, SnapshotterService snapshotterService) {
        this.connectorConfig = connectorConfig;
        this.dispatcher = dispatcher;
        this.connectionFactory = connectionFactory;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.streamingMetrics = streamingMetrics;
        this.snapshotterService = snapshotterService;
    }

    @Override
    public void init(OracleOffsetContext offsetContext) throws InterruptedException {
        this.offsetContext = offsetContext == null ? emptyContext() : offsetContext;
    }

    @Override
    public OracleOffsetContext getOffsetContext() {
        return this.offsetContext;
    }

    private OracleOffsetContext emptyContext() {
        return OracleOffsetContext.create().logicalName(connectorConfig)
                .snapshotPendingTransactions(Collections.emptyMap())
                .transactionContext(new TransactionContext())
                .incrementalSnapshotContext(new SignalBasedIncrementalSnapshotContext<>()).build();
    }

    @Override
    public void execute(ChangeEventSourceContext context, OraclePartition partition, OracleOffsetContext offsetContext) throws InterruptedException {

        try {
            this.partition = partition;
            this.offsetContext = offsetContext;

            final Scn startScn = connectorConfig.getAdapter().getOffsetScn(offsetContext);
            final Long offsetScnIndex = offsetContext.getScnIndex();

            // Resume at the start of the transaction that the offset falls in rather than at the
            // offset itself. OpenLogReplicator sends only what follows the requested position, so
            // asking it to resume mid-transaction relies on it cutting the stream in exactly the
            // right place. Replaying the transaction and discarding what was already emitted, the
            // way the LogMiner adapter does, removes that dependency.
            Long startScnIndex = offsetScnIndex;
            if (offsetScnIndex != null) {
                replayScn = startScn;
                replayScnIndex = offsetScnIndex;
                replayTransactionId = offsetContext.getTransactionId();
                startScnIndex = 0L;
                LOGGER.info("Replaying transaction {} at SCN {} from its start, skipping through index {}.",
                        replayTransactionId, replayScn, replayScnIndex);
            }

            this.client = new OlrNetworkClient(connectorConfig);
            if (client.connect(startScn, startScnIndex)) {
                try {
                    // Start read loop
                    while (client.isConnected() && context.isRunning()) {
                        final StreamingEvent event = client.readEvent();
                        if (event != null) {
                            onEvent(event);
                        }

                        if (context.isPaused()) {
                            LOGGER.info("Streaming will now pause");
                            context.streamingPaused();
                            context.waitSnapshotCompletion();
                            LOGGER.info("Streaming resumed");
                        }
                    }
                }
                finally {
                    try {
                        client.disconnect();
                        LOGGER.info("Client disconnected.");
                    }
                    catch (Exception e) {
                        LOGGER.error("Exception while disconnecting OpenLogReplicator client", e);
                    }
                }
            }
            else {
                LOGGER.warn("Failed to connect to OpenLogReplicator server.");
            }
        }
        catch (Exception e) {
            LOGGER.error("Failed: {}", e.getMessage(), e);
            errorHandler.setProducerThrowable(e);
        }
        finally {
            LOGGER.info("Streaming metrics dump: {}", streamingMetrics.toString());
            LOGGER.info("Offsets: {}", offsetContext);
        }
    }

    @Override
    public void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
        confirmCommittedScn(offset);
    }

    /**
     * Confirms the committed streaming position with OpenLogReplicator, allowing the server to
     * release everything before it.
     *
     * <p>The position has to come from the offset that was committed rather than from the last
     * event that was read. Reads run ahead of what has been delivered, so confirming a position
     * taken from the read loop releases changes the connector has not handed on yet, and those
     * changes are gone if it stops before they are.
     *
     * <p>The offset SCN is the system change number of the transaction the connector was last
     * emitting changes from, so confirming it releases the transactions before that one and keeps
     * that transaction itself available to be replayed. Confirming a position within the
     * transaction would release the part of it that a restart has to read again.
     *
     * <p>Only changes that were dispatched move this position, which is what makes it safe to
     * confirm. The checkpoint markers OpenLogReplicator streams alongside the changes are not
     * ordered against them, so their positions cannot be used here.
     *
     * @param offset the offset that has been committed, never {@code null}
     */
    private void confirmCommittedScn(Map<String, ?> offset) {
        if (client == null || !client.isConnected()) {
            return;
        }

        final Scn scn = OracleOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
        if (scn == null || scn.isNull()) {
            LOGGER.debug("Cannot flush latest offset SCN, no streaming position has been committed yet.");
            return;
        }

        client.confirm(scn, 0L);
    }

    /**
     * Checks whether a change was already emitted before the connector restarted, and so arrived
     * only because streaming rewound to the start of the transaction it belongs to.
     *
     * @param event the event the change was read from, never {@code null}
     * @return {@code true} if the change should be discarded, {@code false} if it should be emitted
     */
    private boolean isAlreadyEmitted(StreamingEvent event) {
        if (replayScn == null) {
            return false;
        }

        final int comparison = event.getCheckpointScn().compareTo(replayScn);
        if (comparison > 0) {
            LOGGER.info("Replay completed, streaming resumes at SCN {}.", event.getCheckpointScn());
            replayScn = null;
            replayScnIndex = null;
            replayTransactionId = null;
            return false;
        }

        if (comparison < 0) {
            // Precedes the transaction being replayed, so it was emitted before the restart.
            return true;
        }

        return Objects.equals(event.getXid(), replayTransactionId)
                && event.getCheckpointIndex() <= replayScnIndex;
    }

    private void onEvent(StreamingEvent event) throws Exception {
        for (PayloadEvent payloadEvent : event.getPayload()) {
            switch (payloadEvent.getType()) {
                case BEGIN:
                    onBeginEvent(event);
                    break;
                case COMMIT:
                    onCommitEvent(event);
                    break;
                case CHECKPOINT:
                    onCheckpointEvent(event);
                    break;
                case DDL:
                    onSchemaChangeEvent(event, (SchemaChangeEvent) payloadEvent);
                    break;
                case INSERT:
                case UPDATE:
                case DELETE:
                    onMutationEvent(event, (AbstractMutationEvent) payloadEvent);
                    break;
                default:
                    throw new DebeziumException("Unexpected event type detected: " + payloadEvent.getType());
            }
        }

        streamingMetrics.incrementProcessedEventsCount();
        streamingMetrics.setCheckpointDetails(event.getCheckpointScn(), event.getCheckpointIndex());
    }

    private void onBeginEvent(StreamingEvent event) {
        // The offset position is only advanced by changes that are dispatched, so that it always
        // describes something the connector has emitted. See #confirmCommittedScn.
        offsetContext.setEventScn(event.getCheckpointScn());
        offsetContext.setSourceTime(event.getTimestamp());
        transactionEvents = false;

        // We do not specifically start a transaction boundary here.
        //
        // This is delayed until the data change event on the first data change that is to be
        // captured by the connector in case there are transactions with events that are not
        // of interest to the connector.
    }

    private void onCommitEvent(StreamingEvent event) throws InterruptedException {
        offsetContext.setEventScn(event.getCheckpointScn());
        offsetContext.setSourceTime(event.getTimestamp());

        streamingMetrics.incrementCommittedTransactionCount();

        // We may see empty transactions and in this case we don't want to emit a transaction boundary
        // record for these cases. Only trigger commit when there are valid changes.
        if (transactionEvents) {
            dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext, event.getTimestamp());
        }

        // For situations where capture tables are changed in-frequently, enabling heartbeats
        // will have a heartbeat emit at commit boundaries even if transaction metadata isn't
        // enabled to guarantee checkpoint offset flushes.
        dispatcher.alwaysDispatchHeartbeatEvent(partition, offsetContext);
    }

    private void onCheckpointEvent(StreamingEvent event) throws InterruptedException {
        // Checkpoint markers track how far OpenLogReplicator has read, which is not ordered against
        // the changes it streams: a transaction is only sent once it commits, and its changes carry
        // the system change number of that commit, which can be lower than a checkpoint that has
        // already been sent. Moving the offset position here would therefore describe the stream as
        // being further along than the changes the connector has actually been given, and the
        // changes still owed would be filtered out as already seen after a restart.
        offsetContext.setEventScn(event.getCheckpointScn());
        offsetContext.setSourceTime(event.getTimestamp());

        dispatcher.alwaysDispatchHeartbeatEvent(partition, offsetContext);
    }

    private void onMutationEvent(StreamingEvent event, AbstractMutationEvent mutationEvent) throws Exception {
        if (isAlreadyEmitted(event)) {
            LOGGER.trace("Skipping change at SCN {} index {}, it has already been emitted.",
                    event.getCheckpointScn(), event.getCheckpointIndex());
            return;
        }

        final Type eventType = mutationEvent.getType();
        final TableId tableId = mutationEvent.getSchema().getTableId(event.getDatabaseName());
        if (!connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
            return;
        }

        Table table = schema.tableFor(tableId);
        if (table == null) {
            Optional<Table> result = potentiallyEmitSchemaChangeForUnknownTable(eventType, tableId);
            if (result.isEmpty()) {
                return;
            }
            table = result.get();
        }

        final Operation operation;
        switch (eventType) {
            case INSERT:
                operation = Operation.CREATE;
                break;
            case UPDATE:
                operation = Operation.UPDATE;
                break;
            case DELETE:
                operation = Operation.DELETE;
                break;
            default:
                throw new DebeziumException("Unexpected DML event type: " + eventType);
        }

        // Update offsets. The position moves here, on a change that is about to be dispatched, so
        // that it always describes something the connector has emitted. The index identifies the
        // change within its transaction and is what a replay skips through on restart.
        offsetContext.setScn(event.getCheckpointScn());
        offsetContext.setScnIndex(event.getCheckpointIndex());
        offsetContext.setEventScn(event.getCheckpointScn());
        offsetContext.setTransactionId(event.getXid());
        offsetContext.tableEvent(tableId, event.getTimestamp());
        offsetContext.setRowId(mutationEvent.getRid());

        streamingMetrics.setLastCapturedDmlCount(1);

        if (!transactionEvents) {
            // First data change that is of interest to the connector, emit the transaction start.
            dispatcher.dispatchTransactionStartedEvent(partition, event.getXid(), offsetContext, event.getTimestamp());
            transactionEvents = true;
        }

        final Object[] oldValues = toColumnValuesArray(table, mutationEvent.getBefore());
        final Object[] newValues = toColumnValuesArray(table, mutationEvent.getAfter());

        LOGGER.trace("Dispatching {} (SCN {}) for table {}", eventType, event.getScn(), tableId);
        dispatcher.dispatchDataChangeEvent(
                partition,
                tableId,
                new OpenLogReplicatorChangeRecordEmitter(
                        connectorConfig,
                        partition,
                        offsetContext,
                        operation,
                        oldValues,
                        newValues,
                        table,
                        schema,
                        clock));
    }

    private void onSchemaChangeEvent(StreamingEvent event, SchemaChangeEvent schemaEvent) throws Exception {
        if (isAlreadyEmitted(event)) {
            LOGGER.trace("Skipping schema change at SCN {} index {}, it has already been emitted.",
                    event.getCheckpointScn(), event.getCheckpointIndex());
            return;
        }

        final PayloadSchema payloadSchema = schemaEvent.getSchema();

        final TableId tableId = payloadSchema.getTableId(event.getDatabaseName());
        if (tableId.schema() == null || tableId.table().startsWith("OBJ_")) {
            LOGGER.trace("Cannot process DDL due to missing schema: {}", schemaEvent.getSql());
            return;
        }
        else if (tableId.table().startsWith("BIN$") && tableId.table().endsWith("==$0")) {
            LOGGER.trace("Skipping DDL for recycling bin table: {}", schemaEvent.getSql());
            return;
        }

        offsetContext.setScn(event.getCheckpointScn());
        offsetContext.setScnIndex(event.getCheckpointIndex());
        offsetContext.setEventScn(event.getCheckpointScn());
        offsetContext.setTransactionId(event.getXid());
        offsetContext.tableEvent(tableId, event.getTimestamp());

        final String sqlStatement = schemaEvent.getSql().toLowerCase().trim();

        // todo: do we want to let other ddl statements be emitted for non-tables?
        if (!isTableSqlStatement(sqlStatement)) {
            LOGGER.trace("Skipping internal DDL: {}", schemaEvent.getSql());
            return;
        }

        if (sqlStatement.contains("rename constraint ")) {
            LOGGER.trace("Ignoring constraint rename: {}", schemaEvent.getSql());
            return;
        }
        else if (sqlStatement.contains("rename to \"bin$")) {
            LOGGER.trace("Ignoring table rename to recycling object: {}", schemaEvent.getSql());
            return;
        }

        LOGGER.trace("Dispatching DDL (SCN {}): [{}]", event.getScn(), schemaEvent.getSql());
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
                        schemaEvent.getSql(),
                        schema,
                        event.getTimestamp(),
                        streamingMetrics,
                        () -> processTruncateEvent(event, schemaEvent)));
    }

    private boolean isTableSqlStatement(String sqlStatement) {
        return sqlStatement.startsWith("create table ")
                || sqlStatement.startsWith("alter table ")
                || sqlStatement.startsWith("drop table ")
                || sqlStatement.startsWith("truncate table ");
    }

    private Object[] toColumnValuesArray(Table table, Values values) {
        Object[] results = new Object[table.columns().size()];
        if (values != null) {
            try {
                final TableId tableId = table.id();
                for (Column column : table.columns()) {
                    final int index = column.position() - 1;
                    final Object value = resolveColumnValue(tableId, column, values);
                    LOGGER.trace("Processing column at {} with name {} [jdbcType={}, type={},length={},scale={}] and value {} ({}).",
                            index, column.name(),
                            column.jdbcType(),
                            column.typeName(), column.length(), column.scale().orElse(0),
                            value, value != null ? value.getClass() : "<null>");
                    results[index] = value;
                }
            }
            catch (Exception e) {
                throw new DebeziumException("Failed to create column array values", e);
            }
        }
        return results;
    }

    private Optional<Table> potentiallyEmitSchemaChangeForUnknownTable(Type eventType, TableId tableId) throws Exception {
        if (!connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
            LOGGER.trace("{} for non-captured table {} detected.", eventType, tableId);
            return Optional.empty();
        }

        LOGGER.warn("Fetching schema for table {}, which should already be loaded. " +
                "This may indicate a potential error in your configuration.", tableId);
        final String tableDdl;
        try {
            tableDdl = connectionFactory.mainConnection().getTableMetadataDdl(tableId);
        }
        catch (NonRelationalTableException e) {
            LOGGER.warn("{} The event will be skipped.", e.getMessage());
            streamingMetrics.incrementWarningCount();
            return Optional.empty();
        }

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

        return Optional.ofNullable(schema.tableFor(tableId));
    }

    private void processTruncateEvent(StreamingEvent event, SchemaChangeEvent ddlEvent) throws InterruptedException {
        if (ddlEvent.getSchema() == null) {
            LOGGER.warn("Truncate event ignored, no schema found.");
            return;
        }

        final TableId tableId = ddlEvent.getSchema().getTableId(event.getDatabaseName());
        if (!connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
            LOGGER.warn("Truncate event ignored, table is no included.");
            return;
        }

        Table table = schema.tableFor(tableId);
        if (table == null) {
            try {
                Optional<Table> result = potentiallyEmitSchemaChangeForUnknownTable(ddlEvent.getType(), tableId);
                if (result.isEmpty()) {
                    LOGGER.warn("Truncate ignored, cannot find table relational model");
                    return;
                }
                table = result.get();
            }
            catch (Exception e) {
                LOGGER.warn("Truncate ignored, failed to emit schema change", e);
                return;
            }
        }

        offsetContext.setScn(event.getScn());
        offsetContext.setEventScn(event.getScn());
        offsetContext.setTransactionId(event.getXid());
        offsetContext.tableEvent(tableId, event.getTimestamp());

        LOGGER.trace("Dispatching {} (SCN {}) for table {}", Operation.TRUNCATE, event.getScn(), tableId);
        dispatcher.dispatchDataChangeEvent(
                partition,
                tableId,
                new OpenLogReplicatorChangeRecordEmitter(
                        connectorConfig,
                        partition,
                        offsetContext,
                        Operation.TRUNCATE,
                        new Object[table.columns().size()],
                        new Object[table.columns().size()],
                        table,
                        schema,
                        clock));
    }

    private Object resolveColumnValue(TableId tableId, Column column, Values values) {
        Object value = values.getValues().getOrDefault(column.name(), OracleValueConverters.UNAVAILABLE_VALUE);
        if (value == OracleValueConverters.UNAVAILABLE_VALUE) {
            // If the get returned the unavailable value, the key does not exist.
            // If the column is LOB, return the unavailable value marker.
            // If the column is not an LOB, return null
            final List<Column> lobColumns = schema.getLobColumnsForTable(tableId);
            for (Column lobColumn : lobColumns) {
                if (lobColumn.equals(column)) {
                    return value;
                }
            }
            value = null;
        }
        else if (column.jdbcType() == Types.VARBINARY && value instanceof String stringValue) {
            // OpenLogReplicator sends binary columns hex encoded, as its payload is JSON and
            // cannot carry binary. The decode belongs here rather than in the value converter,
            // because a converter registered for a column through "converters" replaces the value
            // converter instead of running after it, and so would never see the decode.
            try {
                value = RAW.hexString2Bytes(stringValue);
            }
            catch (SQLException e) {
                throw new DebeziumException("Failed to convert HEX string into byte array: " + stringValue, e);
            }
        }
        return value;
    }

}
