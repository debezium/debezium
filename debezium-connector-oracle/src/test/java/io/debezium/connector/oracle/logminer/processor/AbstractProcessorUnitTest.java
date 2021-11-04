/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE;
import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE;
import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleDefaultValueConverter;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.OracleTopicSelector;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.StreamingAdapter.TableNameCaseSensitivity;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Abstract class implementation for all unit tests for {@link LogMinerEventProcessor} implementations.
 *
 * @author Chris Cranford
 */
public abstract class AbstractProcessorUnitTest<T extends AbstractLogMinerEventProcessor> extends AbstractConnectorTest {

    private static final String TRANSACTION_ID_1 = "1234567890";
    private static final String TRANSACTION_ID_2 = "9876543210";

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    protected ChangeEventSourceContext context;
    protected EventDispatcher<TableId> dispatcher;
    protected OracleDatabaseSchema schema;
    protected OracleStreamingChangeEventSourceMetrics metrics;
    protected OraclePartition partition;
    protected OracleOffsetContext offsetContext;
    protected OracleConnection connection;

    @Before
    @SuppressWarnings({ "unchecked" })
    public void before() throws Exception {
        this.context = Mockito.mock(ChangeEventSourceContext.class);
        Mockito.when(this.context.isRunning()).thenReturn(true);

        this.dispatcher = (EventDispatcher<TableId>) Mockito.mock(EventDispatcher.class);
        this.partition = Mockito.mock(OraclePartition.class);
        this.offsetContext = Mockito.mock(OracleOffsetContext.class);
        this.connection = createOracleConnection();
        this.schema = createOracleDatabaseSchema();
        this.metrics = createMetrics(schema);
    }

    protected abstract Configuration.Builder getConfig();

    protected abstract T getProcessor(OracleConnectorConfig connectorConfig);

    protected boolean isTransactionAbandonmentSupported() {
        return true;
    }

    @Test
    public void testCacheIsEmpty() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            assertThat(processor.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    public void testCacheIsNotEmptyWhenTransactionIsAdded() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            assertThat(processor.getTransactionCache().isEmpty()).isFalse();
        }
    }

    @Test
    public void testCacheIsEmptyWhenTransactionIsCommitted() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            final LogMinerEventRow insertRow = getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1);
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(insertRow);
            processor.handleCommit(getCommitLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1));
            assertThat(processor.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    public void testCacheIsEmptyWhenTransactionIsRolledBack() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            processor.handleRollback(getRollbackLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1));
            assertThat(processor.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    public void testCacheIsNotEmptyWhenFirstTransactionIsRolledBack() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_2));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(4L), TRANSACTION_ID_2));
            processor.handleRollback(getRollbackLogMinerEventRow(Scn.valueOf(5L), TRANSACTION_ID_1));
            assertThat(processor.getTransactionCache().isEmpty()).isFalse();
            assertThat(metrics.getRolledBackTransactionIds().contains(TRANSACTION_ID_1)).isTrue();
            assertThat(metrics.getRolledBackTransactionIds().contains(TRANSACTION_ID_2)).isFalse();
        }
    }

    @Test
    public void testCacheIsNotEmptyWhenSecondTransactionIsRolledBack() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_2));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(4L), TRANSACTION_ID_2));
            processor.handleRollback(getRollbackLogMinerEventRow(Scn.valueOf(5L), TRANSACTION_ID_2));
            assertThat(processor.getTransactionCache().isEmpty()).isFalse();
            assertThat(metrics.getRolledBackTransactionIds().contains(TRANSACTION_ID_2)).isTrue();
            assertThat(metrics.getRolledBackTransactionIds().contains(TRANSACTION_ID_1)).isFalse();
        }
    }

    @Test
    public void testCalculateScnWhenTransactionIsCommitted() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            processor.handleCommit(getCommitLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(2L).toString());
            assertThat(metrics.getRolledBackTransactionIds()).isEmpty();
        }
    }

    @Test
    public void testCalculateScnWhenFirstTransactionIsCommitted() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_2));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(4L), TRANSACTION_ID_2));

            processor.handleCommit(getCommitLogMinerEventRow(Scn.valueOf(5L), TRANSACTION_ID_1));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(3L).toString());
            assertThat(metrics.getRolledBackTransactionIds()).isEmpty();

            processor.handleCommit(getCommitLogMinerEventRow(Scn.valueOf(6L), TRANSACTION_ID_2));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(4L).toString());
        }
    }

    @Test
    public void testCalculateScnWhenSecondTransactionIsCommitted() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_2));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(4L), TRANSACTION_ID_2));

            processor.handleCommit(getCommitLogMinerEventRow(Scn.valueOf(5L), TRANSACTION_ID_2));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(1L).toString());
            assertThat(metrics.getRolledBackTransactionIds()).isEmpty();
        }
    }

    @Test
    public void testAbandonOneTransaction() throws Exception {
        if (!isTransactionAbandonmentSupported()) {
            return;
        }

        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            Mockito.when(offsetContext.getScn()).thenReturn(Scn.valueOf(1L));

            Instant changeTime = Instant.now().minus(24, ChronoUnit.HOURS);
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1, changeTime));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1, changeTime));
            processor.abandonTransactions(Duration.ofHours(1L));
            assertThat(processor.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    public void testAbandonTransactionHavingAnotherOne() throws Exception {
        if (!isTransactionAbandonmentSupported()) {
            return;
        }

        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            Mockito.when(offsetContext.getScn()).thenReturn(Scn.valueOf(1L));

            Instant changeTime = Instant.now().minus(24, ChronoUnit.HOURS);
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1, changeTime));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1, changeTime));
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(4L), TRANSACTION_ID_2));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(5L), TRANSACTION_ID_2));
            processor.abandonTransactions(Duration.ofHours(1L));
            assertThat(processor.getTransactionCache().isEmpty()).isFalse();
            assertThat(processor.getTransactionCache().get(TRANSACTION_ID_1)).isNull();
            assertThat(processor.getTransactionCache().get(TRANSACTION_ID_2)).isNotNull();
        }
    }

    private OracleDatabaseSchema createOracleDatabaseSchema() throws Exception {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(getConfig().build());
        final TopicSelector<TableId> topicSelector = OracleTopicSelector.defaultSelector(connectorConfig);
        final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
        final OracleValueConverters converters = new OracleValueConverters(connectorConfig, connection);
        final OracleDefaultValueConverter defaultValueConverter = new OracleDefaultValueConverter(converters, connection);
        final TableNameCaseSensitivity sensitivity = connectorConfig.getAdapter().getTableNameCaseSensitivity(connection);

        final OracleDatabaseSchema schema = new OracleDatabaseSchema(connectorConfig,
                converters,
                defaultValueConverter,
                schemaNameAdjuster,
                topicSelector,
                sensitivity);

        Table table = Table.editor()
                .tableId(TableId.parse("ORCLPDB1.DEBEZIUM.TEST_TABLE"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("DATA").create())
                .create();

        schema.refresh(table);
        return schema;
    }

    private OracleConnection createOracleConnection() throws Exception {
        final ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.next()).thenReturn(true);
        Mockito.when(rs.getFloat(1)).thenReturn(2.f);

        final PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(stmt.executeQuery()).thenReturn(rs);

        final Connection conn = Mockito.mock(Connection.class);
        Mockito.when(conn.prepareStatement(Mockito.any())).thenReturn(stmt);

        OracleConnection connection = Mockito.mock(OracleConnection.class);
        Mockito.when(connection.connection(Mockito.anyBoolean())).thenReturn(conn);
        Mockito.when(connection.singleOptionalValue(anyString(), any())).thenReturn(2.f);
        return connection;
    }

    private OracleStreamingChangeEventSourceMetrics createMetrics(OracleDatabaseSchema schema) throws Exception {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(getConfig().build());
        final OracleTaskContext taskContext = new OracleTaskContext(connectorConfig, schema);

        final ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(Duration.of(DEFAULT_MAX_QUEUE_SIZE, ChronoUnit.MILLIS))
                .maxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                .maxQueueSize(DEFAULT_MAX_QUEUE_SIZE)
                .build();

        return new OracleStreamingChangeEventSourceMetrics(taskContext, queue, null, connectorConfig);
    }

    private LogMinerEventRow getStartLogMinerEventRow(Scn scn, String transactionId) {
        return getStartLogMinerEventRow(scn, transactionId, Instant.now());
    }

    private LogMinerEventRow getStartLogMinerEventRow(Scn scn, String transactionId, Instant changeTime) {
        LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(row.getEventType()).thenReturn(EventType.START);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(scn);
        Mockito.when(row.getChangeTime()).thenReturn(changeTime);
        return row;
    }

    private LogMinerEventRow getCommitLogMinerEventRow(Scn scn, String transactionId) {
        LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(row.getEventType()).thenReturn(EventType.COMMIT);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(scn);
        Mockito.when(row.getChangeTime()).thenReturn(Instant.now());
        return row;
    }

    private LogMinerEventRow getRollbackLogMinerEventRow(Scn scn, String transactionId) {
        LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(row.getEventType()).thenReturn(EventType.ROLLBACK);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(scn);
        Mockito.when(row.getChangeTime()).thenReturn(Instant.now());
        return row;
    }

    private LogMinerEventRow getInsertLogMinerEventRow(Scn scn, String transactionId) {
        return getInsertLogMinerEventRow(scn, transactionId, Instant.now());
    }

    private LogMinerEventRow getInsertLogMinerEventRow(Scn scn, String transactionId, Instant changeTime) {
        LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(row.getEventType()).thenReturn(EventType.INSERT);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(scn);
        Mockito.when(row.getChangeTime()).thenReturn(changeTime);
        Mockito.when(row.getRowId()).thenReturn("1234567890");
        Mockito.when(row.getOperation()).thenReturn("INSERT");
        Mockito.when(row.getTableName()).thenReturn("TEST_TABLE");
        Mockito.when(row.getTableId()).thenReturn(TableId.parse("ORCLPDB1.DEBEZIUM.TEST_TABLE"));
        Mockito.when(row.getRedoSql()).thenReturn("insert into \"DEBEZIUM\".\"TEST_TABLE\"(\"ID\",\"DATA\") values ('1','Test');");
        Mockito.when(row.getRsId()).thenReturn("A.B.C");
        Mockito.when(row.getTablespaceName()).thenReturn("DEBEZIUM");
        Mockito.when(row.getUserName()).thenReturn(TestHelper.SCHEMA_USER);
        return row;
    }

}
