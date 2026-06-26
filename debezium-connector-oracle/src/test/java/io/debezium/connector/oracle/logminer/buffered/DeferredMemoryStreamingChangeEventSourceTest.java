/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE;
import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.DefaultQueueProvider;
import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleDefaultValueConverter;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.RedoThreadState;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.StreamingAdapter.TableNameCaseSensitivity;
import io.debezium.connector.oracle.jdbc.OracleConnectionFactory;
import io.debezium.connector.oracle.jdbc.StandardOracleConnectionFactory;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.AbstractLogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.OffsetActivityMonitor;
import io.debezium.connector.oracle.logminer.buffered.BufferedLogMinerStreamingChangeEventSource.ProcessResult;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

import oracle.jdbc.OracleTypes;
import oracle.sql.CharacterSet;

/**
 * Unit tests for deferred transaction start behavior in buffered LogMiner with memory buffer type.
 *
 * @author Debezium Authors
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER_BUFFERED)
public class DeferredMemoryStreamingChangeEventSourceTest extends AbstractAsyncEngineConnectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeferredMemoryStreamingChangeEventSourceTest.class);

    private static final int OFFSET_ACTIVITY_MONITOR_INACTIVE_THRESHOLD_MS = 25;
    private static final String TRANSACTION_ID_1 = "1234567890";
    private static final String TRANSACTION_ID_2 = "9876543210";

    private ChangeEventSourceContext context;
    private EventDispatcher<OraclePartition, TableId> dispatcher;
    private OracleDatabaseSchema schema;
    private LogMinerStreamingChangeEventSourceMetrics metrics;
    private OracleOffsetContext offsetContext;
    protected OracleConnectionFactory connectionFactory;
    private CommitScn commitScn;

    @BeforeEach
    @SuppressWarnings({ "unchecked" })
    public void before() throws Exception {
        this.context = Mockito.mock(ChangeEventSourceContext.class);
        Mockito.when(this.context.isRunning()).thenReturn(true);

        this.dispatcher = (EventDispatcher<OraclePartition, TableId>) Mockito.mock(EventDispatcher.class);
        this.offsetContext = Mockito.mock(OracleOffsetContext.class);
        this.commitScn = Mockito.spy(CommitScn.valueOf((String) null));
        Mockito.when(this.offsetContext.getCommitScn()).thenReturn(commitScn);
        Mockito.when(this.offsetContext.getSnapshotScn()).thenReturn(Scn.valueOf("1"));
        this.connectionFactory = createOracleConnectionFactory(false);
        this.schema = createOracleDatabaseSchema();
        this.metrics = createMetrics(schema);
    }

    @AfterEach
    void after() {
        if (schema != null) {
            try {
                schema.close();
            }
            finally {
                schema = null;
            }
        }
    }

    private Configuration.Builder getConfig() {
        return TestHelper.defaultConfig()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, OracleConnectorConfig.LogMiningBufferType.MEMORY)
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_DEFERRED_TRANSACTION_START, true)
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, true);
    }

    @Test
    public void testStartEventDoesNotAddTransactionToCache() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));

            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    public void testInsertEventPromotesDeferredTransactionToCache() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            assertThat(source.getTransactionCache().isEmpty()).isTrue();

            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1));

            assertThat(source.getTransactionCache().isEmpty()).isFalse();
            assertThat(source.getTransactionCache().containsTransaction(TRANSACTION_ID_1)).isTrue();
        }
    }

    @Test
    public void testPromotedTransactionRetainsDeferredStartMetadata() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            final Instant startTime = Instant.now().minusSeconds(30);
            final LogMinerEventRow startEvent = getStartLogMinerEventRow(10, TRANSACTION_ID_1, startTime);
            final LogMinerEventRow insertEvent = getInsertLogMinerEventRow(20, TRANSACTION_ID_1, Instant.now());
            Mockito.when(insertEvent.getUserName()).thenReturn(null);
            Mockito.when(insertEvent.getClientId()).thenReturn("client-from-dml");
            Mockito.when(insertEvent.getThread()).thenReturn(7);

            source.processEvent(startEvent);
            source.processEvent(insertEvent);

            final Transaction transaction = source.getTransactionCache().getTransaction(TRANSACTION_ID_1);
            assertThat(transaction).isNotNull();
            assertThat(transaction.getStartScn()).isEqualTo(Scn.valueOf(10));
            assertThat(transaction.getChangeTime()).isEqualTo(startTime);
            assertThat(transaction.getUserName()).isEqualTo(TestHelper.SCHEMA_USER);
            assertThat(transaction.getClientId()).isNull();
            assertThat(transaction.getRedoThreadId()).isEqualTo(1);
        }
    }

    @Test
    public void testCacheIsEmptyWhenDeferredTransactionIsCommittedWithNoDml() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getCommitLogMinerEventRow(2, TRANSACTION_ID_1));

            assertThat(source.getTransactionCache().isEmpty()).isTrue();
            Mockito.verify(commitScn).recordCommit(any(LogMinerEventRow.class));
        }
    }

    @Test
    public void testCacheIsEmptyWhenDeferredTransactionIsRolledBackWithNoDml() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getRollbackLogMinerEventRow(2, TRANSACTION_ID_1));

            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    public void testPartialRollbackMatchesDeferredTransactionByPrefix() throws Exception {
        final String deferredTransactionId = "12345678abcdef01";

        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, deferredTransactionId));

            assertThat(source.getDeferredTransactionCount()).isEqualTo(1);

            source.processEvent(getRollbackLogMinerEventRow(2, "12345678ffffffff"));

            assertThat(source.getDeferredTransactionCount()).isZero();
            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    public void testCacheIsEmptyWhenDeferredTransactionIsCommittedAfterDml() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1));
            source.processEvent(getCommitLogMinerEventRow(3, TRANSACTION_ID_1));

            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    public void testCacheIsEmptyWhenDeferredTransactionIsRolledBackAfterDml() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1));
            source.processEvent(getRollbackLogMinerEventRow(3, TRANSACTION_ID_1));

            assertThat(source.getTransactionCache().isEmpty()).isTrue();
            assertThat(metrics.getRolledBackTransactionIds()).containsExactly(TRANSACTION_ID_1);
        }
    }

    @Test
    public void testCacheIsNotEmptyWhenFirstDeferredTransactionIsRolledBackAndSecondHasDml() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1));
            source.processEvent(getStartLogMinerEventRow(3, TRANSACTION_ID_2));
            source.processEvent(getInsertLogMinerEventRow(4, TRANSACTION_ID_2));
            source.processEvent(getRollbackLogMinerEventRow(5, TRANSACTION_ID_1));

            assertThat(source.getTransactionCache().isEmpty()).isFalse();
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_1)).isNull();
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_2)).isNotNull();
            assertThat(metrics.getRolledBackTransactionIds()).containsExactly(TRANSACTION_ID_1);
        }
    }

    @Test
    public void testMiningWindowIsNotPinnedByCachedTransactions() throws Exception {
        final ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.next()).thenReturn(true, true, false);
        Mockito.when(rs.getString(1)).thenReturn("101", "200");
        Mockito.when(rs.getString(2)).thenReturn(
                "insert into \"DEBEZIUM\".\"ABC\"(\"ID\",\"DATA\") values ('1','test1');",
                "insert into \"DEBEZIUM\".\"ABC\"(\"ID\",\"DATA\") values ('2','test2');");
        Mockito.when(rs.getInt(3)).thenReturn(EventType.INSERT.getValue());
        Mockito.when(rs.getTimestamp(eq(4), any(Calendar.class))).thenReturn(Timestamp.valueOf(LocalDateTime.now()));
        Mockito.when(rs.getString(7)).thenReturn("ABC");
        Mockito.when(rs.getString(8)).thenReturn("DEBEZIUM");
        Mockito.when(rs.getString(10)).thenReturn("AAAAAAAAAAAAAAAAAB", "AAAAAAAAAAAAAAAAAC");
        Mockito.when(rs.getBytes(5)).thenReturn(new byte[]{ 0x12, 0x34, 0x56, 0x78 });

        final PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        Mockito.when(ps.executeQuery()).thenReturn(rs);

        try (var source = getChangeEventSource(getConfig().build())) {
            final BufferedStreamingChangeEventSource mock = Mockito.spy(source);
            Mockito.doReturn(ps).when(mock).createQueryStatement();

            final OracleConnection mainConnection = connectionFactory.streamingConnectionFactory().mainConnection();
            Mockito.when(mainConnection.getTableMetadataDdl(Mockito.any(TableId.class)))
                    .thenReturn("CREATE TABLE DEBEZIUM.ABC (ID primary key(9,0), data varchar2(50))");

            final Table table = Table.editor()
                    .tableId(TableId.parse("ORCLPDB1.DEBEZIUM.ABC"))
                    .addColumn(Column.editor().name("ID").create())
                    .addColumn(Column.editor().name("DATA").create())
                    .setPrimaryKeyNames("ID").create();

            Mockito.doReturn(table)
                    .when(mock)
                    .dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(Mockito.any(TableId.class));

            // Process a transaction with two DML events in the same mining window.
            // The transaction is promoted to the cache on the first DML.
            // Because there is no START event in the window, the transaction's startScn in the cache
            // is the first DML's SCN (101). In non-deferred mode, the mining window would pin to
            // the oldest transaction (101 - 1 = 100). In deferred mode, it should advance block-by-block
            // because lastProcessedScn (200) is not less than endScn (200), so endScn stays at 200
            // and miningSessionStartScn = 200 - 1 = 199.
            final ProcessResult result = mock.process(Scn.valueOf(100), Scn.valueOf(100), Scn.valueOf(200));

            assertThat(result.miningSessionStartScn()).isEqualTo(Scn.valueOf(199));
            assertThat(result.readStartScn()).isEqualTo(Scn.valueOf(200));
        }
    }

    @Test
    // Verifies the getOldestDeferredTransactionStartScn() helper's lifecycle (add on START,
    // stays min after rollback, NULL when empty). Offset behaviour that consumes this helper
    // is covered by testOffsetScnPinnedToMinOfDeferredAndCachedTransactions.
    public void testOldestDeferredTransactionStartScnLifecycle() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(100, TRANSACTION_ID_1));
            source.processEvent(getStartLogMinerEventRow(150, TRANSACTION_ID_2));

            assertThat(source.getOldestDeferredTransactionStartScn()).isEqualTo(Scn.valueOf(100));

            source.processEvent(getRollbackLogMinerEventRow(160, TRANSACTION_ID_2));

            assertThat(source.getOldestDeferredTransactionStartScn()).isEqualTo(Scn.valueOf(100));

            source.processEvent(getRollbackLogMinerEventRow(170, TRANSACTION_ID_1));

            assertThat(source.getOldestDeferredTransactionStartScn()).isEqualTo(Scn.NULL);
        }
    }

    @Test
    public void testOffsetScnPinnedToMinOfDeferredAndCachedTransactions() throws Exception {
        final ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.next()).thenReturn(true, false);
        Mockito.when(rs.getString(1)).thenReturn("200");
        Mockito.when(rs.getString(2)).thenReturn(
                "insert into \"DEBEZIUM\".\"ABC\"(\"ID\",\"DATA\") values ('3','test3');");
        Mockito.when(rs.getInt(3)).thenReturn(EventType.INSERT.getValue());
        Mockito.when(rs.getTimestamp(eq(4), any(Calendar.class))).thenReturn(Timestamp.valueOf(LocalDateTime.now()));
        Mockito.when(rs.getString(7)).thenReturn("ABC");
        Mockito.when(rs.getString(8)).thenReturn("DEBEZIUM");
        Mockito.when(rs.getString(10)).thenReturn("AAAAAAAAAAAAAAAAAD");
        Mockito.when(rs.getBytes(5)).thenReturn(new byte[]{ 0x12, 0x34, 0x56, 0x78 });

        final PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        Mockito.when(ps.executeQuery()).thenReturn(rs);

        Mockito.when(commitScn.getMaxCommittedScn()).thenReturn(Scn.valueOf(500));

        try (var source = getChangeEventSource(getConfig().build())) {
            final BufferedStreamingChangeEventSource mock = Mockito.spy(source);
            Mockito.doReturn(ps).when(mock).createQueryStatement();

            final OracleConnection mainConnection = connectionFactory.streamingConnectionFactory().mainConnection();
            Mockito.when(mainConnection.getTableMetadataDdl(Mockito.any(TableId.class)))
                    .thenReturn("CREATE TABLE DEBEZIUM.ABC (ID primary key(9,0), data varchar2(50))");

            final Table table = Table.editor()
                    .tableId(TableId.parse("ORCLPDB1.DEBEZIUM.ABC"))
                    .addColumn(Column.editor().name("ID").create())
                    .addColumn(Column.editor().name("DATA").create())
                    .setPrimaryKeyNames("ID").create();

            Mockito.doReturn(table)
                    .when(mock)
                    .dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(Mockito.any(TableId.class));

            // Promote TRANSACTION_ID_1 into the cache with startScn=100
            source.processEvent(getStartLogMinerEventRow(100, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(110, TRANSACTION_ID_1));

            // Leave TRANSACTION_ID_2 in the deferred map with startScn=150
            source.processEvent(getStartLogMinerEventRow(150, TRANSACTION_ID_2));

            assertThat(source.getTransactionCache().containsTransaction(TRANSACTION_ID_1)).isTrue();
            assertThat(source.getDeferredTransactionCount()).isEqualTo(1);

            // process() triggers calculateNewStartScn. The offset SCN should be
            // min(oldestDeferredScn=150, minCacheScn=100) - 1 = 99, not just 150.
            // The subtract(ONE) matches the non-deferred path so the START event at
            // the oldest SCN is included by the mining query's exclusive SCN > ? bound.
            final ProcessResult result = mock.process(Scn.valueOf(100), Scn.valueOf(100), Scn.valueOf(200));

            Mockito.verify(offsetContext).setScn(Scn.valueOf(99));
            assertThat(result.miningSessionStartScn()).isEqualTo(Scn.valueOf(199));
            assertThat(result.readStartScn()).isEqualTo(Scn.valueOf(200));
        }
    }

    @Test
    public void testMultipleDeferredTransactionsOnlyPromotedOnDml() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getStartLogMinerEventRow(2, TRANSACTION_ID_2));

            assertThat(source.getTransactionCache().isEmpty()).isTrue();

            source.processEvent(getInsertLogMinerEventRow(3, TRANSACTION_ID_1));

            assertThat(source.getTransactionCache().containsTransaction(TRANSACTION_ID_1)).isTrue();
            assertThat(source.getTransactionCache().containsTransaction(TRANSACTION_ID_2)).isFalse();
        }
    }

    @Test
    public void testDeferredTransactionsAreCleanedUpByRetention() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getStartLogMinerEventRow(3, TRANSACTION_ID_2));

            assertThat(source.getDeferredTransactionCount()).isEqualTo(2);

            source.cleanupDeferredTransactionsForTest(Duration.ofHours(1));

            assertThat(source.getDeferredTransactionCount()).isEqualTo(1);
            assertThat(source.hasDeferredTransaction(TRANSACTION_ID_1)).isFalse();
            assertThat(source.hasDeferredTransaction(TRANSACTION_ID_2)).isTrue();
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private OracleDatabaseSchema createOracleDatabaseSchema() throws Exception {
        Configuration configuration = getConfig().build();
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(configuration);
        final TopicNamingStrategy topicNamingStrategy = SchemaTopicNamingStrategy.create(connectorConfig);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();
        final OracleConnection connection = connectionFactory.mainConnection();
        final OracleValueConverters converters = connectorConfig.getAdapter().getValueConverter(connectorConfig, connection);
        final OracleDefaultValueConverter defaultValueConverter = new OracleDefaultValueConverter(converters, connection);
        final TableNameCaseSensitivity sensitivity = connectorConfig.getAdapter().getTableNameCaseSensitivity(connection);

        final OracleDatabaseSchema schema = new OracleDatabaseSchema(connectorConfig,
                converters,
                defaultValueConverter,
                schemaNameAdjuster,
                topicNamingStrategy,
                sensitivity,
                false, new CustomConverterRegistry(emptyList()), new OracleTaskContext(configuration, connectorConfig));

        Table table = Table.editor()
                .tableId(TableId.parse("ORCLPDB1.DEBEZIUM.TEST_TABLE"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("DATA").create())
                .create();

        Table lobTable = Table.editor()
                .tableId(TableId.parse("ORCLPDB1.DEBEZIUM.TEST_LOB_TABLE"))
                .addColumn(Column.editor().name("ID").type("VARCHAR2(50)").create())
                .addColumn(Column.editor().name("DATA").type("CLOB").jdbcType(OracleTypes.CLOB).create())
                .setPrimaryKeyNames("ID")
                .create();

        schema.refresh(table);
        schema.refresh(lobTable);
        return schema;
    }

    private OracleConnectionFactory createOracleConnectionFactory(boolean singleOptionalValueThrowException) throws Exception {
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
        Mockito.when(connection.getDatabaseCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.AL32UTF8_CHARSET));
        if (!singleOptionalValueThrowException) {
            Mockito.when(connection.singleOptionalValue(anyString(), any())).thenReturn(BigInteger.TWO);
        }
        else {
            Mockito.when(connection.singleOptionalValue(anyString(), any()))
                    .thenThrow(new SQLException("ORA-01555 Snapshot too old", null, 1555));
        }
        Mockito.when(connection.isArchiveLogDestinationValid(eq("LOG_ARCHIVE_DEST_1"))).thenReturn(true);

        final OracleConnectionFactory factory = Mockito.mock(StandardOracleConnectionFactory.class);
        Mockito.when(factory.mainConnection()).thenReturn(connection);
        Mockito.when(factory.streamingConnectionFactory()).thenReturn(factory);

        return factory;
    }

    private LogMinerStreamingChangeEventSourceMetrics createMetrics(OracleDatabaseSchema schema) throws Exception {
        final Configuration config = getConfig().build();
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final OracleTaskContext taskContext = new OracleTaskContext(config, connectorConfig);

        final ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(Duration.of(DEFAULT_MAX_QUEUE_SIZE, ChronoUnit.MILLIS))
                .maxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                .maxQueueSize(DEFAULT_MAX_QUEUE_SIZE)
                .queueProvider(createDefaultQueueProvider(DEFAULT_MAX_QUEUE_SIZE))
                .build();

        return new LogMinerStreamingChangeEventSourceMetrics(taskContext, queue, null, connectorConfig, Collections::emptyList);
    }

    private static DefaultQueueProvider<DataChangeEvent> createDefaultQueueProvider(int maxQueueSize) {
        DefaultQueueProvider<DataChangeEvent> provider = new DefaultQueueProvider<>();
        provider.configure(Map.of("max.queue.size", String.valueOf(maxQueueSize)));
        return provider;
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
        Mockito.when(row.getUserName()).thenReturn(TestHelper.SCHEMA_USER);
        Mockito.when(row.getClientId()).thenReturn(null);
        Mockito.when(row.getThread()).thenReturn(1);
        return row;
    }

    private LogMinerEventRow getCommitLogMinerEventRow(long scn, String transactionId) {
        LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(row.getEventType()).thenReturn(EventType.COMMIT);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(Scn.valueOf(scn));
        Mockito.when(row.getChangeTime()).thenReturn(Instant.now());
        Mockito.when(row.getThread()).thenReturn(1);
        return row;
    }

    private LogMinerEventRow getRollbackLogMinerEventRow(long scn, String transactionId) {
        LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(row.getEventType()).thenReturn(EventType.ROLLBACK);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(Scn.valueOf(scn));
        Mockito.when(row.getChangeTime()).thenReturn(Instant.now());
        Mockito.when(row.getThread()).thenReturn(1);
        return row;
    }

    private LogMinerEventRow getInsertLogMinerEventRow(long scn, String transactionId) {
        return getInsertLogMinerEventRow(scn, transactionId, Instant.now());
    }

    private LogMinerEventRow getInsertLogMinerEventRow(long scn, String transactionId, Instant changeTime) {
        return getInsertLogMinerEventRow(scn, transactionId, changeTime, "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'Test'");
    }

    private LogMinerEventRow getInsertLogMinerEventRow(long scn, String transactionId, Instant changeTime, String tableName, String rowId, String dataValue) {
        LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(row.getEventType()).thenReturn(EventType.INSERT);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(Scn.valueOf(scn));
        Mockito.when(row.getChangeTime()).thenReturn(changeTime);
        Mockito.when(row.getRowId()).thenReturn(rowId);
        Mockito.when(row.getOperation()).thenReturn("INSERT");
        Mockito.when(row.getTableName()).thenReturn(tableName);
        Mockito.when(row.getTableId()).thenReturn(TableId.parse("ORCLPDB1.DEBEZIUM." + tableName));
        Mockito.when(row.getRedoSql()).thenReturn("insert into \"DEBEZIUM\".\"%s\"(\"ID\",\"DATA\") values ('1',%s);".formatted(tableName, dataValue));
        Mockito.when(row.getRsId()).thenReturn("A.B.C");
        Mockito.when(row.getTablespaceName()).thenReturn("DEBEZIUM");
        Mockito.when(row.getUserName()).thenReturn(TestHelper.SCHEMA_USER);
        Mockito.when(row.getClientId()).thenReturn(null);
        Mockito.when(row.getThread()).thenReturn(1);
        return row;
    }

    private static RedoThreadState buildRedoThreadState(int threadId, String enabled) {
        return RedoThreadState.builder()
                .thread()
                .threadId(threadId)
                .status("OPEN")
                .enabled(enabled)
                .logGroups(2L)
                .instanceName("ORCLCDB")
                .openTime(Instant.now())
                .currentGroupNumber(1L)
                .currentSequenceNumber(1L)
                .checkpointScn(Scn.valueOf(1))
                .checkpointTime(Instant.now())
                .enabledScn(Scn.valueOf(1))
                .enabledTime(Instant.now())
                .disabledScn(Scn.valueOf(0))
                .disabledTime(null)
                .lastRedoSequenceNumber(1L)
                .lastRedoBlock(1L)
                .lastRedoScn(Scn.valueOf(1))
                .lastRedoTime(Instant.now())
                .conId(0L)
                .build()
                .build();
    }

    protected BufferedStreamingChangeEventSource getChangeEventSource(Configuration config) throws Exception {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        assertThat(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error)).isTrue();

        final BufferedStreamingChangeEventSource source = new BufferedStreamingChangeEventSource(
                connectorConfig,
                connectionFactory,
                dispatcher,
                schema,
                metrics,
                context,
                offsetContext);

        source.init(offsetContext);

        source.setCurrentRedoThreadState(buildRedoThreadState(1, "PUBLIC"));

        return source;
    }

    // Helper class that permits exposing some protected methods for mocking
    protected static class BufferedStreamingChangeEventSource extends BufferedLogMinerStreamingChangeEventSource {

        private final ChangeEventSourceContext context;
        private final OffsetActivityMonitor offsetActivityMonitor;

        public BufferedStreamingChangeEventSource(
                                                  OracleConnectorConfig connectorConfig,
                                                  OracleConnectionFactory connectionFactory,
                                                  EventDispatcher<OraclePartition, TableId> dispatcher,
                                                  OracleDatabaseSchema schema,
                                                  LogMinerStreamingChangeEventSourceMetrics metrics,
                                                  ChangeEventSourceContext context,
                                                  OracleOffsetContext offsetContext) {
            super(connectorConfig, connectionFactory, dispatcher, null, Clock.SYSTEM, schema, connectorConfig.getJdbcConfig(), metrics);
            this.context = context;
            this.offsetActivityMonitor = new OffsetActivityMonitor(OFFSET_ACTIVITY_MONITOR_INACTIVE_THRESHOLD_MS, offsetContext, metrics);
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

        public void setCurrentRedoThreadState(RedoThreadState state) throws Exception {
            var field = AbstractLogMinerStreamingChangeEventSource.class.getDeclaredField("currentRedoThreadState");
            field.setAccessible(true);
            field.set(this, state);
        }

        public int getDeferredTransactionCount() {
            return getDeferredTransactionsForTest().size();
        }

        public boolean hasDeferredTransaction(String transactionId) {
            return getDeferredTransactionsForTest().containsKey(transactionId);
        }

        public Scn getOldestDeferredTransactionStartScn() {
            try {
                final var method = BufferedLogMinerStreamingChangeEventSource.class.getDeclaredMethod("getOldestDeferredTransactionStartScn");
                method.setAccessible(true);
                return (Scn) method.invoke(this);
            }
            catch (ReflectiveOperationException e) {
                throw new AssertionError("Unable to invoke getOldestDeferredTransactionStartScn", e);
            }
        }

        public void cleanupDeferredTransactionsForTest(Duration retention) {
            try {
                final var method = BufferedLogMinerStreamingChangeEventSource.class.getDeclaredMethod("cleanupDeferredTransactions", Duration.class);
                method.setAccessible(true);
                method.invoke(this, retention);
            }
            catch (ReflectiveOperationException e) {
                throw new AssertionError("Unable to invoke deferred transaction cleanup", e);
            }
        }

        @SuppressWarnings("unchecked")
        private Map<String, ?> getDeferredTransactionsForTest() {
            try {
                final var field = BufferedLogMinerStreamingChangeEventSource.class.getDeclaredField("deferredTransactions");
                field.setAccessible(true);
                return (Map<String, ?>) field.get(this);
            }
            catch (ReflectiveOperationException e) {
                throw new AssertionError("Unable to read deferred transaction state", e);
            }
        }
    }
}
