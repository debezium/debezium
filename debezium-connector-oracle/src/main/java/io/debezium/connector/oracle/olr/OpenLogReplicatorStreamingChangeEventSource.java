/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.Scn;
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
import io.debezium.util.Clock;

/**
 * An implementation of {@link StreamingChangeEventSource} based on OpenLogReplicator.
 *
 * @author Chris Cranford
 */
public class OpenLogReplicatorStreamingChangeEventSource implements StreamingChangeEventSource<OraclePartition, OracleOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenLogReplicatorStreamingChangeEventSource.class);

    private final Map<Integer, DateTimeFormatter> timestampWithTimeZoneFormatterCache = new HashMap<>();
    private final Map<Integer, DateTimeFormatter> timestampWithLocalTimeZoneFormatterCache = new HashMap<>();
    private final OracleConnectorConfig connectorConfig;
    private final OracleConnection jdbcConnection;
    private final EventDispatcher<OraclePartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final OracleStreamingChangeEventSourceMetrics streamingMetrics;

    private OraclePartition partition;
    private OracleOffsetContext offsetContext;
    private long transactionEvents = 0;

    public OpenLogReplicatorStreamingChangeEventSource(OracleConnectorConfig connectorConfig, OracleConnection connection,
                                                       EventDispatcher<OraclePartition, TableId> dispatcher,
                                                       ErrorHandler errorHandler, Clock clock,
                                                       OracleDatabaseSchema schema,
                                                       OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this.connectorConfig = connectorConfig;
        this.dispatcher = dispatcher;
        this.jdbcConnection = connection;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.streamingMetrics = streamingMetrics;
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
        if (!connectorConfig.getSnapshotMode().shouldStream()) {
            LOGGER.info("Streaming is not enabled in current configuration");
            return;
        }
        try {
            this.partition = partition;
            this.offsetContext = offsetContext;
            this.jdbcConnection.setAutoCommit(false);

            final Scn startScn = offsetContext.getScn();
            final OlrNetworkClient client = new OlrNetworkClient(connectorConfig);
            if (client.connect(startScn)) {
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

                client.disconnect();
                LOGGER.info("Client disconnected.");
            }
            else {
                LOGGER.warn("Failed to connect to OpenLogReplicator server.");
            }
        }
        catch (Exception e) {
            LOGGER.error("Failed: {}", e.getMessage(), e);
            errorHandler.setProducerThrowable(e);
        }
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
    }

    private void onBeginEvent(StreamingEvent event) {
        final String transactionId = event.getXid();
        final Instant timestamp = Instant.ofEpochMilli(Long.parseLong(event.getTimestamp()));

        offsetContext.setScn(Scn.valueOf(event.getScn()));
        offsetContext.setEventScn(Scn.valueOf(event.getScn()));
        offsetContext.setTransactionId(transactionId);
        offsetContext.setSourceTime(timestamp);
        transactionEvents = 0;

        streamingMetrics.setOffsetScn(offsetContext.getScn());
        streamingMetrics.setActiveTransactions(1);

        // We do not specifically start a transaction boundary here.
        //
        // This is delayed until the data change event on the first data change that is to be
        // captured by the connector in case there are transactions with events that are not
        // of interest to the connector.
    }

    private void onCommitEvent(StreamingEvent event) throws InterruptedException {
        final String transactionId = event.getXid();
        final Instant timestamp = Instant.ofEpochMilli(Long.parseLong(event.getTimestamp()));

        offsetContext.setScn(Scn.valueOf(event.getScn()));
        offsetContext.setEventScn(Scn.valueOf(event.getScn()));
        offsetContext.setTransactionId(transactionId);
        offsetContext.setSourceTime(timestamp);

        streamingMetrics.setOffsetScn(offsetContext.getScn());
        streamingMetrics.setCommittedScn(offsetContext.getScn());
        streamingMetrics.setActiveTransactions(0);
        streamingMetrics.incrementCommittedTransactions();

        // We may see empty transactions and in this case we don't want to emit a transaction boundary
        // record for these cases. Only trigger commit when there are valid changes.
        if (transactionEvents > 0) {
            dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext, timestamp);
        }
    }

    private void onCheckpointEvent(StreamingEvent event) {
        final String transactionId = event.getXid();
        final Instant timestamp = Instant.ofEpochMilli(Long.parseLong(event.getTimestamp()));

        offsetContext.setScn(Scn.valueOf(event.getScn()));
        offsetContext.setEventScn(Scn.valueOf(event.getScn()));
        offsetContext.setTransactionId(transactionId);
        offsetContext.setSourceTime(timestamp);

        streamingMetrics.setOffsetScn(offsetContext.getScn());
        streamingMetrics.setCommittedScn(offsetContext.getScn());
    }

    private void onMutationEvent(StreamingEvent event, AbstractMutationEvent mutationEvent) throws Exception {
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

        final Instant timestamp = Instant.ofEpochMilli(Long.parseLong(event.getTimestamp()));
        if (transactionEvents == 0) {
            // First data change that is of interest to the connector, emit the transaction start.
            dispatcher.dispatchTransactionStartedEvent(partition, event.getXid(), offsetContext, timestamp);
        }

        // Update offsets
        offsetContext.setScn(Scn.valueOf(event.getScn()));
        offsetContext.setEventScn(Scn.valueOf(event.getScn()));
        offsetContext.setTransactionId(event.getXid());
        offsetContext.tableEvent(tableId, timestamp);

        streamingMetrics.setOffsetScn(offsetContext.getScn());
        streamingMetrics.addProcessedRows(1L);
        streamingMetrics.setLastCapturedDmlCount(1);
        streamingMetrics.incrementRegisteredDmlCount();

        transactionEvents++;

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

        final Instant timestamp = Instant.ofEpochMilli(Long.parseLong(event.getTimestamp()));
        offsetContext.setScn(Scn.valueOf(event.getScn()));
        offsetContext.setEventScn(Scn.valueOf(event.getScn()));
        offsetContext.setTransactionId(event.getXid());
        offsetContext.tableEvent(tableId, timestamp);

        streamingMetrics.setOffsetScn(offsetContext.getScn());
        streamingMetrics.setCommittedScn(offsetContext.getScn());
        streamingMetrics.addProcessedRows(1L);

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
                        Instant.ofEpochMilli(Long.parseLong(event.getTimestamp())),
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
            tableDdl = jdbcConnection.getTableMetadataDdl(tableId);
        }
        catch (NonRelationalTableException e) {
            LOGGER.warn("Table {} is not a relational table, the {} will be skipped.", tableId, eventType);
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

        final Instant timestamp = Instant.ofEpochMilli(Long.parseLong(event.getTimestamp()));

        offsetContext.setScn(Scn.valueOf(event.getScn()));
        offsetContext.setEventScn(Scn.valueOf(event.getScn()));
        offsetContext.setTransactionId(event.getXid());
        offsetContext.tableEvent(tableId, timestamp);

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
        return value;
    }

}
