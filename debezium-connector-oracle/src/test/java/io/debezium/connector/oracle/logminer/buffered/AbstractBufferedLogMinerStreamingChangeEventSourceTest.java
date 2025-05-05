/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import io.debezium.connector.oracle.logminer.OffsetActivityMonitor;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

import oracle.sql.CharacterSet;

/**
 * Abstract class implementation for all unit tests for {@link BufferedLogMinerStreamingChangeEventSource}.
 *
 * @author Chris Cranford
 */
public abstract class AbstractBufferedLogMinerStreamingChangeEventSourceTest extends AbstractAsyncEngineConnectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBufferedLogMinerStreamingChangeEventSourceTest.class);

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

    protected boolean isTransactionAbandonmentSupported() {
        return true;
    }

    @Test
    public void testCacheIsEmpty() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    public void testCacheIsNotEmptyWhenTransactionIsAdded() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(1, TRANSACTION_ID_1));

            assertThat(source.getTransactionCache().isEmpty()).isFalse();
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testCacheIsNotEmptyWhenTransactionIsAddedAndStartEventIsNotHandled() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getInsertLogMinerEventRow(1, TRANSACTION_ID_1));

            assertThat(source.getTransactionCache().isEmpty()).isFalse();
        }
    }

    @Test
    public void testCacheIsEmptyWhenTransactionIsCommitted() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1));
            source.processEvent(getCommitLogMinerEventRow(3, TRANSACTION_ID_1));

            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testCacheIsEmptyWhenTransactionIsCommittedAndStartEventIsNotHandled() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getInsertLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getCommitLogMinerEventRow(2, TRANSACTION_ID_1));

            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    public void testCacheIsEmptyWhenTransactionIsRolledBack() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1));
            source.processEvent(getRollbackLogMinerEventRow(3, TRANSACTION_ID_1));

            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testCacheIsEmptyWhenTransactionIsRolledBackAndStartEventIsNotHandled() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getInsertLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getRollbackLogMinerEventRow(2, TRANSACTION_ID_1));

            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    public void testCacheIsNotEmptyWhenFirstTransactionIsRolledBack() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1));
            source.processEvent(getStartLogMinerEventRow(3, TRANSACTION_ID_2));
            source.processEvent(getInsertLogMinerEventRow(4, TRANSACTION_ID_2));
            source.processEvent(getRollbackLogMinerEventRow(5, TRANSACTION_ID_1));

            assertThat(source.getTransactionCache().isEmpty()).isFalse();
            assertThat(metrics.getRolledBackTransactionIds()).containsExactly(TRANSACTION_ID_1);
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testCacheIsNotEmptyWhenFirstTransactionIsRolledBackAndStartEventIsNotHandled() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getInsertLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_2));
            source.processEvent(getRollbackLogMinerEventRow(3, TRANSACTION_ID_1));

            assertThat(source.getTransactionCache().isEmpty()).isFalse();
            assertThat(metrics.getRolledBackTransactionIds()).containsExactly(TRANSACTION_ID_1);
        }
    }

    @Test
    public void testCacheIsNotEmptyWhenSecondTransactionIsRolledBack() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1));
            source.processEvent(getStartLogMinerEventRow(3, TRANSACTION_ID_2));
            source.processEvent(getInsertLogMinerEventRow(4, TRANSACTION_ID_2));
            source.processEvent(getRollbackLogMinerEventRow(5, TRANSACTION_ID_2));

            assertThat(source.getTransactionCache().isEmpty()).isFalse();
            assertThat(metrics.getRolledBackTransactionIds()).containsExactly(TRANSACTION_ID_2);
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testCacheIsNotEmptyWhenSecondTransactionIsRolledBackAndStartEventIsNotHandled() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getInsertLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_2));
            source.processEvent(getRollbackLogMinerEventRow(3, TRANSACTION_ID_2));

            assertThat(source.getTransactionCache().isEmpty()).isFalse();
            assertThat(metrics.getRolledBackTransactionIds()).containsExactly(TRANSACTION_ID_2);
        }
    }

    @Test
    public void testCalculateScnWhenTransactionIsCommitted() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1));
            source.processEvent(getCommitLogMinerEventRow(3, TRANSACTION_ID_1));

            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(2L).asBigInteger());
            assertThat(metrics.getRolledBackTransactionIds()).isEmpty();
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testCalculateScnWhenTransactionIsCommittedAndStartEventIsNotHandled() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getInsertLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getCommitLogMinerEventRow(2, TRANSACTION_ID_1));

            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(1L).asBigInteger());
            assertThat(metrics.getRolledBackTransactionIds()).isEmpty();
        }
    }

    @Test
    public void testCalculateScnWhenFirstTransactionIsCommitted() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1));
            source.processEvent(getStartLogMinerEventRow(3, TRANSACTION_ID_2));
            source.processEvent(getInsertLogMinerEventRow(4, TRANSACTION_ID_2));

            source.processEvent(getCommitLogMinerEventRow(5, TRANSACTION_ID_1));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(3L).asBigInteger());
            assertThat(metrics.getRolledBackTransactionIds()).isEmpty();

            source.processEvent(getCommitLogMinerEventRow(6, TRANSACTION_ID_2));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(4L).asBigInteger());
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testCalculateScnWhenFirstTransactionIsCommittedAndStartEventIsNotHandled() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getInsertLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_2));

            source.processEvent(getCommitLogMinerEventRow(3, TRANSACTION_ID_1));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(2L).asBigInteger());
            assertThat(metrics.getRolledBackTransactionIds()).isEmpty();

            source.processEvent(getCommitLogMinerEventRow(4, TRANSACTION_ID_2));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(2L).asBigInteger());
        }
    }

    @Test
    public void testCalculateScnWhenSecondTransactionIsCommitted() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1));
            source.processEvent(getStartLogMinerEventRow(3, TRANSACTION_ID_2));
            source.processEvent(getInsertLogMinerEventRow(4, TRANSACTION_ID_2));
            source.processEvent(getCommitLogMinerEventRow(5, TRANSACTION_ID_2));

            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(1L).asBigInteger());
            assertThat(metrics.getRolledBackTransactionIds()).isEmpty();
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testCalculateScnWhenSecondTransactionIsCommittedAndStartEventIsNotHandled() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getInsertLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_2));
            source.processEvent(getCommitLogMinerEventRow(3, TRANSACTION_ID_2));

            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(1L).asBigInteger());
            assertThat(metrics.getRolledBackTransactionIds()).isEmpty();
        }
    }

    @Test
    @FixFor("DBZ-6679")
    public void testEmptyResultSetWithMineRangeAdvancesCorrectly() throws Exception {
        if (!isTransactionAbandonmentSupported()) {
            return;
        }

        try (var source = getChangeEventSource(getConfig().build())) {
            final ResultSet rs = Mockito.mock(ResultSet.class);
            Mockito.when(rs.next()).thenReturn(false);

            final PreparedStatement ps = Mockito.mock(PreparedStatement.class);
            Mockito.when(ps.executeQuery()).thenReturn(rs);

            final BufferedLogMinerStreamingChangeEventSource mock = Mockito.spy(source);
            Mockito.doReturn(ps).when(mock).createQueryStatement();

            final Scn nextStartScn = mock.process(Scn.valueOf(100), Scn.valueOf(200));
            assertThat(nextStartScn).isEqualTo(Scn.valueOf(100));
        }
    }

    @Test
    @FixFor("DBZ-6679")
    public void testNonEmptyResultSetWithMineRangeAdvancesCorrectly() throws Exception {
        if (!isTransactionAbandonmentSupported()) {
            return;
        }

        try (var source = getChangeEventSource(getConfig().build())) {
            final ResultSet rs = Mockito.mock(ResultSet.class);
            Mockito.when(rs.next()).thenReturn(true, false);
            Mockito.when(rs.getString(1)).thenReturn("101");
            Mockito.when(rs.getString(2)).thenReturn("insert into \"DEBEZIUM\".\"ABC\"(\"ID\",\"DATA\") values ('1','test');");
            Mockito.when(rs.getInt(3)).thenReturn(EventType.INSERT.getValue());
            Mockito.when(rs.getString(7)).thenReturn("ABC");
            Mockito.when(rs.getString(8)).thenReturn("DEBEZIUM");

            final PreparedStatement ps = Mockito.mock(PreparedStatement.class);
            Mockito.when(ps.executeQuery()).thenReturn(rs);

            final BufferedStreamingChangeEventSource mock = Mockito.spy(source);
            Mockito.doReturn(ps).when(mock).createQueryStatement();

            Mockito.doReturn("CREATE TABLE DEBEZIUM.ABC (ID primary key(9,0), data varchar2(50))")
                    .when(connection)
                    .getTableMetadataDdl(Mockito.any(TableId.class));

            final Table table = Table.editor()
                    .tableId(TableId.parse(TestHelper.getDatabaseName() + ".DEBEZIUM.ABC"))
                    .addColumn(Column.editor().name("ID").create())
                    .addColumn(Column.editor().name("DATA").create())
                    .setPrimaryKeyNames("ID").create();

            Mockito.doReturn(table)
                    .when(mock)
                    .dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(Mockito.any(TableId.class));

            final Scn nextStartScn = mock.process(Scn.valueOf(100), Scn.valueOf(200));
            assertThat(nextStartScn).isEqualTo(Scn.valueOf(101));
        }
    }

    @Test
    public void testAbandonOneTransaction() throws Exception {
        if (!isTransactionAbandonmentSupported()) {
            return;
        }

        try (var source = getChangeEventSource(getConfig().build())) {
            Mockito.when(offsetContext.getScn()).thenReturn(Scn.valueOf(1L));
            Mockito.when(offsetContext.getSnapshotScn()).thenReturn(Scn.NULL);

            final Instant changeTime = Instant.now().minus(24, ChronoUnit.HOURS);

            source.processEvent(getStartLogMinerEventRow(2, TRANSACTION_ID_1, changeTime));
            source.processEvent(getInsertLogMinerEventRow(3, TRANSACTION_ID_1, changeTime));
            source.abandonTransactions(Duration.ofHours(1L));

            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testAbandonOneTransactionAndStartEventIsNotHandled() throws Exception {
        if (!isTransactionAbandonmentSupported()) {
            return;
        }

        try (var source = getChangeEventSource(getConfig().build())) {
            Mockito.when(offsetContext.getScn()).thenReturn(Scn.valueOf(1L));
            Mockito.when(offsetContext.getSnapshotScn()).thenReturn(Scn.NULL);

            final Instant changeTime = Instant.now().minus(24, ChronoUnit.HOURS);
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1, changeTime));
            source.abandonTransactions(Duration.ofHours(1L));

            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    public void testAbandonTransactionHavingAnotherOne() throws Exception {
        if (!isTransactionAbandonmentSupported()) {
            return;
        }

        try (var source = getChangeEventSource(getConfig().build())) {
            Mockito.when(offsetContext.getScn()).thenReturn(Scn.valueOf(1L));
            Mockito.when(offsetContext.getSnapshotScn()).thenReturn(Scn.NULL);

            final Instant changeTime = Instant.now().minus(24, ChronoUnit.HOURS);
            source.processEvent(getStartLogMinerEventRow(2, TRANSACTION_ID_1, changeTime));
            source.processEvent(getInsertLogMinerEventRow(3, TRANSACTION_ID_1, changeTime));
            source.processEvent(getStartLogMinerEventRow(4, TRANSACTION_ID_2));
            source.processEvent(getInsertLogMinerEventRow(5, TRANSACTION_ID_2));
            source.abandonTransactions(Duration.ofHours(1L));

            assertThat(source.getTransactionCache().isEmpty()).isFalse();
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_1)).isNull();
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_2)).isNotNull();
        }
    }

    @Test
    @FixFor("DBZ-7473")
    public void testAbandonTransactionHavingAnotherOneAndStartEventIsNotHandled() throws Exception {
        if (!isTransactionAbandonmentSupported()) {
            return;
        }

        try (var source = getChangeEventSource(getConfig().build())) {
            Mockito.when(offsetContext.getScn()).thenReturn(Scn.valueOf(1L));
            Mockito.when(offsetContext.getSnapshotScn()).thenReturn(Scn.NULL);

            final Instant changeTime = Instant.now().minus(24, ChronoUnit.HOURS);
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1, changeTime));
            source.processEvent(getInsertLogMinerEventRow(3, TRANSACTION_ID_2));
            source.abandonTransactions(Duration.ofHours(1L));

            assertThat(source.getTransactionCache().isEmpty()).isFalse();
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_1)).isNull();
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_2)).isNotNull();
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

        Mockito.when(offsetContext.getScn()).thenReturn(Scn.valueOf(1L));
        Mockito.when(offsetContext.getSnapshotScn()).thenReturn(Scn.NULL);

        final Instant changeTime1 = Instant.now().minus(24, ChronoUnit.HOURS);
        final Instant changeTime2 = Instant.now().minus(23, ChronoUnit.HOURS);

        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(2, TRANSACTION_ID_1, changeTime1));
            source.processEvent(getInsertLogMinerEventRow(3, TRANSACTION_ID_1, changeTime1));
            source.processEvent(getStartLogMinerEventRow(4, TRANSACTION_ID_2, changeTime2));
            source.processEvent(getInsertLogMinerEventRow(5, TRANSACTION_ID_2, changeTime2));
            source.processEvent(getStartLogMinerEventRow(6, TRANSACTION_ID_3));
            source.processEvent(getInsertLogMinerEventRow(7, TRANSACTION_ID_3));
            source.abandonTransactions(Duration.ofHours(1L));

            assertThat(source.getTransactionCache().isEmpty()).isFalse();
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_1)).isNull();
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_2)).isNull();
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_3)).isNotNull();
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

        Mockito.when(offsetContext.getScn()).thenReturn(Scn.valueOf(1L));
        Mockito.when(offsetContext.getSnapshotScn()).thenReturn(Scn.NULL);

        final Instant changeTime1 = Instant.now().minus(24, ChronoUnit.HOURS);
        final Instant changeTime2 = Instant.now().minus(23, ChronoUnit.HOURS);

        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1, changeTime1));
            source.processEvent(getInsertLogMinerEventRow(3, TRANSACTION_ID_2, changeTime2));
            source.processEvent(getInsertLogMinerEventRow(4, TRANSACTION_ID_3));
            source.abandonTransactions(Duration.ofHours(1L));

            assertThat(source.getTransactionCache().isEmpty()).isFalse();
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_1)).isNull();
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_2)).isNull();
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_3)).isNotNull();
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
                sensitivity,
                false);

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

    private LogMinerEventRow getStartLogMinerEventRow(long scn, String transactionId) {
        return getStartLogMinerEventRow(scn, transactionId, Instant.now());
    }

    private LogMinerEventRow getStartLogMinerEventRow(long scn, String transactionId, Instant changeTime) {
        LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(row.getEventType()).thenReturn(EventType.START);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(Scn.valueOf(scn));
        Mockito.when(row.getChangeTime()).thenReturn(changeTime);
        return row;
    }

    private LogMinerEventRow getCommitLogMinerEventRow(long scn, String transactionId) {
        LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(row.getEventType()).thenReturn(EventType.COMMIT);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(Scn.valueOf(scn));
        Mockito.when(row.getChangeTime()).thenReturn(Instant.now());
        return row;
    }

    private LogMinerEventRow getRollbackLogMinerEventRow(long scn, String transactionId) {
        LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(row.getEventType()).thenReturn(EventType.ROLLBACK);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(Scn.valueOf(scn));
        Mockito.when(row.getChangeTime()).thenReturn(Instant.now());
        return row;
    }

    private LogMinerEventRow getInsertLogMinerEventRow(long scn, String transactionId) {
        return getInsertLogMinerEventRow(scn, transactionId, Instant.now());
    }

    private LogMinerEventRow getInsertLogMinerEventRow(long scn, String transactionId, Instant changeTime) {
        LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(row.getEventType()).thenReturn(EventType.INSERT);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(Scn.valueOf(scn));
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

    protected BufferedStreamingChangeEventSource getChangeEventSource(Configuration config) throws InterruptedException {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        assertThat(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error)).isTrue();

        final BufferedStreamingChangeEventSource source = new BufferedStreamingChangeEventSource(
                connectorConfig,
                connection,
                dispatcher,
                schema,
                metrics,
                context,
                offsetContext);

        source.init(offsetContext);

        return source;
    }

    // Helper class that permits exposing some protected methods for mocking
    protected static class BufferedStreamingChangeEventSource extends BufferedLogMinerStreamingChangeEventSource {

        private final ChangeEventSourceContext context;
        private final OffsetActivityMonitor offsetActivityMonitor;

        public BufferedStreamingChangeEventSource(
                                                  OracleConnectorConfig connectorConfig,
                                                  OracleConnection connection,
                                                  EventDispatcher<OraclePartition, TableId> dispatcher,
                                                  OracleDatabaseSchema schema,
                                                  LogMinerStreamingChangeEventSourceMetrics metrics,
                                                  ChangeEventSourceContext context,
                                                  OracleOffsetContext offsetContext) {
            super(connectorConfig, connection, dispatcher, null, Clock.SYSTEM, schema, connectorConfig.getJdbcConfig(), metrics);
            this.context = context;
            this.offsetActivityMonitor = new OffsetActivityMonitor(25, offsetContext, metrics);
        }

        @Override
        protected ChangeEventSourceContext getContext() {
            // Necessary for mock purposes only
            return context;
        }

        @Override
        protected OffsetActivityMonitor getOffsetActivityMonitor() {
            // Necessary for mock purposes only
            return offsetActivityMonitor;
        }

        @Override
        public Table dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(TableId tableId) throws SQLException, InterruptedException {
            // Necessary for mock purposes only
            return super.dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(tableId);
        }

        @Override
        public void processEvent(LogMinerEventRow event) throws SQLException, InterruptedException {
            // Necessary for mock purposes only
            super.processEvent(event);
        }
    }
}
