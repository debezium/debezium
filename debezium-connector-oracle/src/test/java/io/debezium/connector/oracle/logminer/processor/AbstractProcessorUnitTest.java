/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE;
import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleDefaultValueConverter;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.StreamingAdapter.TableNameCaseSensitivity;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.spi.topic.TopicNamingStrategy;

import oracle.sql.CharacterSet;

/**
 * Abstract class implementation for all unit tests for {@link LogMinerEventProcessor} implementations.
 *
 * @author Chris Cranford
 */
public abstract class AbstractProcessorUnitTest<T extends AbstractLogMinerEventProcessor> extends AbstractConnectorTest {

    private static final String TRANSACTION_ID_1 = "1234567890";
    private static final String TRANSACTION_ID_2 = "9876543210";
    private static final String TRANSACTION_ID_3 = "9880212345";

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    protected ChangeEventSourceContext context;
    protected EventDispatcher<OraclePartition, TableId> dispatcher;
    protected OracleDatabaseSchema schema;
    protected LogMinerStreamingChangeEventSourceMetrics metrics;
    protected OraclePartition partition;
    protected OracleOffsetContext offsetContext;
    protected OracleConnection connection;

    @Before
    @SuppressWarnings({ "unchecked" })
    public void before() throws Exception {
        this.context = Mockito.mock(ChangeEventSourceContext.class);
        Mockito.when(this.context.isRunning()).thenReturn(true);

        this.dispatcher = (EventDispatcher<OraclePartition, TableId>) Mockito.mock(EventDispatcher.class);
        this.partition = Mockito.mock(OraclePartition.class);
        this.offsetContext = Mockito.mock(OracleOffsetContext.class);
        final CommitScn commitScn = CommitScn.valueOf((String) null);
        Mockito.when(this.offsetContext.getCommitScn()).thenReturn(commitScn);
        Mockito.when(this.offsetContext.getSnapshotScn()).thenReturn(Scn.valueOf("1"));
        this.connection = createOracleConnection(false);
        this.schema = createOracleDatabaseSchema();
        this.metrics = createMetrics(schema);
    }

    @After
    public void after() {
        if (schema != null) {
            try {
                schema.close();
            }
            finally {
                schema = null;
            }
        }
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
            assertThat(processor.transactionCacheKeys().isEmpty()).isTrue();
        }
    }

    @Test
    public void testCacheIsNotEmptyWhenTransactionIsAdded() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            assertThat(processor.transactionCacheKeys().isEmpty()).isFalse();
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testCacheIsNotEmptyWhenTransactionIsAddedAndStartEventIsNotHandled() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            assertThat(processor.transactionCacheKeys().isEmpty()).isFalse();
        }
    }

    @Test
    public void testCacheIsEmptyWhenTransactionIsCommitted() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        final OraclePartition partition = new OraclePartition(config.getLogicalName(), config.getDatabaseName());
        try (T processor = getProcessor(config)) {
            final LogMinerEventRow insertRow = getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1);
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(insertRow);
            processor.handleCommit(partition, getCommitLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1));
            assertThat(processor.transactionCacheKeys().isEmpty()).isTrue();
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testCacheIsEmptyWhenTransactionIsCommittedAndStartEventIsNotHandled() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        final OraclePartition partition = new OraclePartition(config.getLogicalName(), config.getDatabaseName());
        try (T processor = getProcessor(config)) {
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleCommit(partition, getCommitLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            assertThat(processor.transactionCacheKeys().isEmpty()).isTrue();
        }
    }

    @Test
    public void testCacheIsEmptyWhenTransactionIsRolledBack() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            processor.handleRollback(getRollbackLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1));
            assertThat(processor.transactionCacheKeys().isEmpty()).isTrue();
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testCacheIsEmptyWhenTransactionIsRolledBackAndStartEventIsNotHandled() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleRollback(getRollbackLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            assertThat(processor.transactionCacheKeys().isEmpty()).isTrue();
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
            assertThat(processor.transactionCacheKeys().isEmpty()).isFalse();
            assertThat(metrics.getRolledBackTransactionIds().contains(TRANSACTION_ID_1)).isTrue();
            assertThat(metrics.getRolledBackTransactionIds().contains(TRANSACTION_ID_2)).isFalse();
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testCacheIsNotEmptyWhenFirstTransactionIsRolledBackAndStartEventIsNotHandled() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_2));
            processor.handleRollback(getRollbackLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1));
            assertThat(processor.transactionCacheKeys().isEmpty()).isFalse();
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
            assertThat(processor.transactionCacheKeys().isEmpty()).isFalse();
            assertThat(metrics.getRolledBackTransactionIds().contains(TRANSACTION_ID_2)).isTrue();
            assertThat(metrics.getRolledBackTransactionIds().contains(TRANSACTION_ID_1)).isFalse();
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testCacheIsNotEmptyWhenSecondTransactionIsRolledBackAndStartEventIsNotHandled() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_2));
            processor.handleRollback(getRollbackLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_2));
            assertThat(processor.transactionCacheKeys().isEmpty()).isFalse();
            assertThat(metrics.getRolledBackTransactionIds().contains(TRANSACTION_ID_2)).isTrue();
            assertThat(metrics.getRolledBackTransactionIds().contains(TRANSACTION_ID_1)).isFalse();
        }
    }

    @Test
    public void testCalculateScnWhenTransactionIsCommitted() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        final OraclePartition partition = new OraclePartition(config.getLogicalName(), config.getDatabaseName());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            processor.handleCommit(partition, getCommitLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(2L).toString());
            assertThat(metrics.getRolledBackTransactionIds()).isEmpty();
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testCalculateScnWhenTransactionIsCommittedAndStartEventIsNotHandled() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        final OraclePartition partition = new OraclePartition(config.getLogicalName(), config.getDatabaseName());
        try (T processor = getProcessor(config)) {
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleCommit(partition, getCommitLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(1L).toString());
            assertThat(metrics.getRolledBackTransactionIds()).isEmpty();
        }
    }

    @Test
    public void testCalculateScnWhenFirstTransactionIsCommitted() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        final OraclePartition partition = new OraclePartition(config.getLogicalName(), config.getDatabaseName());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_2));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(4L), TRANSACTION_ID_2));

            processor.handleCommit(partition, getCommitLogMinerEventRow(Scn.valueOf(5L), TRANSACTION_ID_1));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(3L).toString());
            assertThat(metrics.getRolledBackTransactionIds()).isEmpty();

            processor.handleCommit(partition, getCommitLogMinerEventRow(Scn.valueOf(6L), TRANSACTION_ID_2));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(4L).toString());
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testCalculateScnWhenFirstTransactionIsCommittedAndStartEventIsNotHandled() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        final OraclePartition partition = new OraclePartition(config.getLogicalName(), config.getDatabaseName());
        try (T processor = getProcessor(config)) {
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_2));

            processor.handleCommit(partition, getCommitLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(2L).toString());
            assertThat(metrics.getRolledBackTransactionIds()).isEmpty();

            processor.handleCommit(partition, getCommitLogMinerEventRow(Scn.valueOf(4L), TRANSACTION_ID_2));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(2L).toString());
        }
    }

    @Test
    public void testCalculateScnWhenSecondTransactionIsCommitted() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        final OraclePartition partition = new OraclePartition(config.getLogicalName(), config.getDatabaseName());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_2));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(4L), TRANSACTION_ID_2));

            processor.handleCommit(partition, getCommitLogMinerEventRow(Scn.valueOf(5L), TRANSACTION_ID_2));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(1L).toString());
            assertThat(metrics.getRolledBackTransactionIds()).isEmpty();
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testCalculateScnWhenSecondTransactionIsCommittedAndStartEventIsNotHandled() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        final OraclePartition partition = new OraclePartition(config.getLogicalName(), config.getDatabaseName());
        try (T processor = getProcessor(config)) {
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_2));

            processor.handleCommit(partition, getCommitLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_2));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(1L).toString());
            assertThat(metrics.getRolledBackTransactionIds()).isEmpty();
        }
    }

    @Test
    @FixFor("DBZ-6679")
    public void testEmptyResultSetWithMineRangeAdvancesCorrectly() throws Exception {
        if (!isTransactionAbandonmentSupported()) {
            return;
        }

        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            final ResultSet rs = Mockito.mock(ResultSet.class);
            Mockito.when(rs.next()).thenReturn(false);

            final PreparedStatement ps = Mockito.mock(PreparedStatement.class);
            Mockito.when(processor.createQueryStatement()).thenReturn(ps);
            Mockito.when(ps.executeQuery()).thenReturn(rs);

            Scn nextStartScn = processor.process(Scn.valueOf(100), Scn.valueOf(200));
            assertThat(nextStartScn).isEqualTo(Scn.valueOf(100));
        }
    }

    @Test
    @FixFor("DBZ-6679")
    public void testNonEmptyResultSetWithMineRangeAdvancesCorrectly() throws Exception {
        if (!isTransactionAbandonmentSupported()) {
            return;
        }

        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            final ResultSet rs = Mockito.mock(ResultSet.class);
            Mockito.when(rs.next()).thenReturn(true, false);
            Mockito.when(rs.getString(1)).thenReturn("101");
            Mockito.when(rs.getString(2)).thenReturn("insert into \"DEBEZIUM\".\"ABC\"(\"ID\",\"DATA\") values ('1','test');");
            Mockito.when(rs.getInt(3)).thenReturn(EventType.INSERT.getValue());
            Mockito.when(rs.getString(7)).thenReturn("ABC");
            Mockito.when(rs.getString(8)).thenReturn("DEBEZIUM");

            final PreparedStatement ps = Mockito.mock(PreparedStatement.class);
            Mockito.when(processor.createQueryStatement()).thenReturn(ps);
            Mockito.when(ps.executeQuery()).thenReturn(rs);

            T processorMock = Mockito.spy(processor);
            Mockito.doReturn("CREATE TABLE DEBEZIUM.ABC (ID primary key(9,0), data varchar2(50))")
                    .when(processorMock)
                    .getTableMetadataDdl(Mockito.any(OracleConnection.class), Mockito.any(TableId.class));

            final Table table = Table.editor()
                    .tableId(TableId.parse(TestHelper.getDatabaseName() + ".DEBEZIUM.ABC"))
                    .addColumn(Column.editor().name("ID").create())
                    .addColumn(Column.editor().name("DATA").create())
                    .setPrimaryKeyNames("ID").create();

            Mockito.doReturn(table)
                    .when(processorMock)
                    .dispatchSchemaChangeEventAndGetTableForNewCapturedTable(
                            Mockito.any(TableId.class),
                            Mockito.any(OracleOffsetContext.class),
                            Mockito.any(EventDispatcher.class));

            Scn nextStartScn = processorMock.process(Scn.valueOf(100), Scn.valueOf(200));
            assertThat(nextStartScn).isEqualTo(Scn.valueOf(101));
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
            Mockito.when(offsetContext.getSnapshotScn()).thenReturn(Scn.NULL);

            Instant changeTime = Instant.now().minus(24, ChronoUnit.HOURS);
            processor.processRow(partition, getStartLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1, changeTime));
            processor.processRow(partition, getInsertLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1, changeTime));
            processor.abandonTransactions(Duration.ofHours(1L));
            assertThat(processor.transactionCacheKeys().isEmpty()).isTrue();
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testAbandonOneTransactionAndStartEventIsNotHandled() throws Exception {
        if (!isTransactionAbandonmentSupported()) {
            return;
        }

        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            Mockito.when(offsetContext.getScn()).thenReturn(Scn.valueOf(1L));
            Mockito.when(offsetContext.getSnapshotScn()).thenReturn(Scn.NULL);

            Instant changeTime = Instant.now().minus(24, ChronoUnit.HOURS);
            processor.processRow(partition, getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1, changeTime));
            processor.abandonTransactions(Duration.ofHours(1L));
            assertThat(processor.transactionCacheKeys().isEmpty()).isTrue();
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
            Mockito.when(offsetContext.getSnapshotScn()).thenReturn(Scn.NULL);

            Instant changeTime = Instant.now().minus(24, ChronoUnit.HOURS);
            processor.processRow(partition, getStartLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1, changeTime));
            processor.processRow(partition, getInsertLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1, changeTime));
            processor.processRow(partition, getStartLogMinerEventRow(Scn.valueOf(4L), TRANSACTION_ID_2));
            processor.processRow(partition, getInsertLogMinerEventRow(Scn.valueOf(5L), TRANSACTION_ID_2));
            processor.abandonTransactions(Duration.ofHours(1L));
            assertThat(processor.transactionCacheKeys().isEmpty()).isFalse();
            assertThat(processor.transactionCacheGet(TRANSACTION_ID_1)).isNull();
            assertThat(processor.transactionCacheGet(TRANSACTION_ID_2)).isNotNull();
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testAbandonTransactionHavingAnotherOneAndStartEventIsNotHandled() throws Exception {
        if (!isTransactionAbandonmentSupported()) {
            return;
        }

        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            Mockito.when(offsetContext.getScn()).thenReturn(Scn.valueOf(1L));
            Mockito.when(offsetContext.getSnapshotScn()).thenReturn(Scn.NULL);

            Instant changeTime = Instant.now().minus(24, ChronoUnit.HOURS);
            processor.processRow(partition, getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1, changeTime));
            processor.processRow(partition, getInsertLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_2));
            processor.abandonTransactions(Duration.ofHours(1L));
            assertThat(processor.transactionCacheKeys().isEmpty()).isFalse();
            assertThat(processor.transactionCacheGet(TRANSACTION_ID_1)).isNull();
            assertThat(processor.transactionCacheGet(TRANSACTION_ID_2)).isNotNull();
        }
    }

    @Test
    @FixFor("DBZ-6355")
    public void testAbandonTransactionsUsingFallbackBasedOnChangeTime() throws Exception {
        if (!isTransactionAbandonmentSupported()) {
            return;
        }

        // re-create some mocked objects
        this.schema.close();

        connection = createOracleConnection(true);
        schema = createOracleDatabaseSchema();
        metrics = createMetrics(schema);

        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            Mockito.when(offsetContext.getScn()).thenReturn(Scn.valueOf(1L));
            Mockito.when(offsetContext.getSnapshotScn()).thenReturn(Scn.NULL);

            Instant changeTime1 = Instant.now().minus(24, ChronoUnit.HOURS);
            Instant changeTime2 = Instant.now().minus(23, ChronoUnit.HOURS);
            processor.processRow(partition, getStartLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1, changeTime1));
            processor.processRow(partition, getInsertLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1, changeTime1));
            processor.processRow(partition, getStartLogMinerEventRow(Scn.valueOf(4L), TRANSACTION_ID_2, changeTime2));
            processor.processRow(partition, getInsertLogMinerEventRow(Scn.valueOf(5L), TRANSACTION_ID_2, changeTime2));
            processor.processRow(partition, getStartLogMinerEventRow(Scn.valueOf(6L), TRANSACTION_ID_3));
            processor.processRow(partition, getInsertLogMinerEventRow(Scn.valueOf(7L), TRANSACTION_ID_3));
            processor.abandonTransactions(Duration.ofHours(1L));
            assertThat(processor.transactionCacheKeys().isEmpty()).isFalse();
            assertThat(processor.transactionCacheGet(TRANSACTION_ID_1)).isNull();
            assertThat(processor.transactionCacheGet(TRANSACTION_ID_2)).isNull();
            assertThat(processor.transactionCacheGet(TRANSACTION_ID_3)).isNotNull();
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testAbandonTransactionsUsingFallbackBasedOnChangeTimeAndStartEventIsNotHandled() throws Exception {
        if (!isTransactionAbandonmentSupported()) {
            return;
        }

        // re-create some mocked objects
        this.schema.close();

        connection = createOracleConnection(true);
        schema = createOracleDatabaseSchema();
        metrics = createMetrics(schema);

        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        try (T processor = getProcessor(config)) {
            Mockito.when(offsetContext.getScn()).thenReturn(Scn.valueOf(1L));
            Mockito.when(offsetContext.getSnapshotScn()).thenReturn(Scn.NULL);

            Instant changeTime1 = Instant.now().minus(24, ChronoUnit.HOURS);
            Instant changeTime2 = Instant.now().minus(23, ChronoUnit.HOURS);
            processor.processRow(partition, getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1, changeTime1));
            processor.processRow(partition, getInsertLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_2, changeTime2));
            processor.processRow(partition, getInsertLogMinerEventRow(Scn.valueOf(4L), TRANSACTION_ID_3));
            processor.abandonTransactions(Duration.ofHours(1L));
            assertThat(processor.transactionCacheKeys().isEmpty()).isFalse();
            assertThat(processor.transactionCacheGet(TRANSACTION_ID_1)).isNull();
            assertThat(processor.transactionCacheGet(TRANSACTION_ID_2)).isNull();
            assertThat(processor.transactionCacheGet(TRANSACTION_ID_3)).isNotNull();
        }

    }

    private OracleDatabaseSchema createOracleDatabaseSchema() throws Exception {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(getConfig().build());
        final TopicNamingStrategy topicNamingStrategy = SchemaTopicNamingStrategy.create(connectorConfig);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();
        final OracleValueConverters converters = connectorConfig.getAdapter().getValueConverter(connectorConfig, connection);
        final OracleDefaultValueConverter defaultValueConverter = new OracleDefaultValueConverter(converters, connection);
        final TableNameCaseSensitivity sensitivity = connectorConfig.getAdapter().getTableNameCaseSensitivity(connection);

        final OracleDatabaseSchema schema = new OracleDatabaseSchema(connectorConfig,
                converters,
                defaultValueConverter,
                schemaNameAdjuster,
                topicNamingStrategy,
                sensitivity);

        Table table = Table.editor()
                .tableId(TableId.parse("ORCLPDB1.DEBEZIUM.TEST_TABLE"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("DATA").create())
                .create();

        schema.refresh(table);
        return schema;
    }

    private OracleConnection createOracleConnection(boolean singleOptionalValueThrowException) throws Exception {
        final ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.next()).thenReturn(true);
        Mockito.when(rs.getFloat(1)).thenReturn(2.f);

        final PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(stmt.executeQuery()).thenReturn(rs);

        final Connection conn = Mockito.mock(Connection.class);
        Mockito.when(conn.prepareStatement(Mockito.any())).thenReturn(stmt);

        OracleConnection connection = Mockito.mock(OracleConnection.class);
        Mockito.when(connection.connection(Mockito.anyBoolean())).thenReturn(conn);
        Mockito.when(connection.connection()).thenReturn(conn);
        Mockito.when(connection.getNationalCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.UTF8_CHARSET));
        if (!singleOptionalValueThrowException) {
            Mockito.when(connection.singleOptionalValue(anyString(), any())).thenReturn(BigInteger.TWO);
        }
        else {
            Mockito.when(connection.singleOptionalValue(anyString(), any()))
                    .thenThrow(new SQLException("ORA-01555 Snapshot too old", null, 1555));
        }
        return connection;
    }

    private LogMinerStreamingChangeEventSourceMetrics createMetrics(OracleDatabaseSchema schema) throws Exception {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(getConfig().build());
        final OracleTaskContext taskContext = new OracleTaskContext(connectorConfig, schema);

        final ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(Duration.of(DEFAULT_MAX_QUEUE_SIZE, ChronoUnit.MILLIS))
                .maxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                .maxQueueSize(DEFAULT_MAX_QUEUE_SIZE)
                .build();

        return new LogMinerStreamingChangeEventSourceMetrics(taskContext, queue, null, connectorConfig);
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
