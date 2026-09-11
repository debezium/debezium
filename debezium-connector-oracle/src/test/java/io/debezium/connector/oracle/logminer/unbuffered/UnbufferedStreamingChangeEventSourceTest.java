/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.unbuffered;

import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE;
import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE;
import static io.debezium.config.CommonConnectorConfig.DEFAULT_POLL_DISPATCH_INTERVAL_MILLIS;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
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
import io.debezium.connector.oracle.OracleConnectorConfig.ConnectorAdapter;
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
import io.debezium.connector.oracle.logminer.TransactionCommitConsumer;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope.Operation;
import io.debezium.doc.FixFor;
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

import oracle.sql.CharacterSet;

/**
 * Unit tests for {@link UnbufferedLogMinerStreamingChangeEventSource}.
 * <p>
 * The buffered implementation is covered by
 * {@link io.debezium.connector.oracle.logminer.buffered.AbstractBufferedLogMinerStreamingChangeEventSourceTest},
 * but that suite is pinned to the buffered adapter and therefore never exercises this implementation.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER_UNBUFFERED)
public class UnbufferedStreamingChangeEventSourceTest extends AbstractAsyncEngineConnectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnbufferedStreamingChangeEventSourceTest.class);

    private static final String TABLE_NAME = "TEST_TABLE";
    private static final String TRANSACTION_ID_1 = "1234567890";
    private static final String ROW_ID = "AAAAAAAAAAAAAAAAAB";
    private static final int THREAD_ID = 1;

    protected ChangeEventSourceContext context;
    protected EventDispatcher<OraclePartition, TableId> dispatcher;
    protected OracleDatabaseSchema schema;
    protected LogMinerStreamingChangeEventSourceMetrics metrics;
    protected OraclePartition partition;
    protected OracleOffsetContext offsetContext;
    protected OracleConnectionFactory connectionFactory;

    @BeforeEach
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
        this.connectionFactory = createOracleConnectionFactory();
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

    protected Configuration.Builder getConfig() {
        return TestHelper.defaultConfig()
                .with(OracleConnectorConfig.CONNECTOR_ADAPTER, ConnectorAdapter.LOG_MINER_UNBUFFERED.getValue());
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testDataChangeEventIsDispatched() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getUpdateLogMinerEventRow(2, 5, TRANSACTION_ID_1, false));

            // Control for the rollback test below: the same row without the flag reaches the accumulator and is dispatched
            assertThat(getAccumulator(source).getTotalEvents()).isEqualTo(1);
            Mockito.verify(dispatcher, Mockito.times(1))
                    .dispatchDataChangeEvent(any(), any(), argThat(emitter -> emitter.getOperation() == Operation.UPDATE));
        }
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testDataChangeEventWithRollbackFlagIsNotDispatched() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getUpdateLogMinerEventRow(2, 5, TRANSACTION_ID_1, true));

            // Unbuffered mode only reads committed transactions, so a ROLLBACK=1 row describes an undo
            // that LogMiner has already applied. There is nothing to undo on our side, and the row
            // itself must not be emitted as a change.
            assertThat(getAccumulator(source).getTotalEvents()).isZero();
            Mockito.verify(dispatcher, Mockito.never()).dispatchDataChangeEvent(any(), any(), any());
        }
    }

    private LogMinerEventRow getUpdateLogMinerEventRow(long scn, long commitScn, String transactionId, boolean rollbackFlag) {
        final LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(row.getEventType()).thenReturn(EventType.UPDATE);
        Mockito.when(row.isRollbackFlag()).thenReturn(rollbackFlag);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(Scn.valueOf(scn));
        Mockito.when(row.getCommitScn()).thenReturn(Scn.valueOf(commitScn));
        Mockito.when(row.getThread()).thenReturn(THREAD_ID);
        Mockito.when(row.getChangeTime()).thenReturn(Instant.now());
        Mockito.when(row.getRowId()).thenReturn(ROW_ID);
        Mockito.when(row.getOperation()).thenReturn("UPDATE");
        Mockito.when(row.getTableName()).thenReturn(TABLE_NAME);
        Mockito.when(row.getTableId()).thenReturn(TableId.parse("ORCLPDB1.DEBEZIUM." + TABLE_NAME));
        Mockito.when(row.getRedoSql()).thenReturn(
                "update \"DEBEZIUM\".\"%s\" set \"DATA\" = 'update' where \"ID\" = '1' and ROWID = '%s';".formatted(TABLE_NAME, ROW_ID));
        Mockito.when(row.getRsId()).thenReturn("A.B.C");
        Mockito.when(row.getTablespaceName()).thenReturn("DEBEZIUM");
        Mockito.when(row.getUserName()).thenReturn(TestHelper.SCHEMA_USER);
        return row;
    }

    protected UnbufferedStreamingChangeEventSource getChangeEventSource(Configuration config) throws Exception {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        assertThat(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error)).isTrue();

        final UnbufferedStreamingChangeEventSource source = new UnbufferedStreamingChangeEventSource(
                connectorConfig,
                connectionFactory,
                dispatcher,
                schema,
                metrics,
                context);

        source.init(offsetContext);

        // Both are normally assigned by executeLogMiningStreaming, which these tests bypass by
        // feeding rows to processEvent directly.
        setDatabaseOffset(source, ZoneOffset.UTC);
        setCurrentRedoThreadState(source, buildRedoThreadState(THREAD_ID));

        return source;
    }

    private void setCurrentRedoThreadState(UnbufferedLogMinerStreamingChangeEventSource source, RedoThreadState state) throws Exception {
        final var field = AbstractLogMinerStreamingChangeEventSource.class.getDeclaredField("currentRedoThreadState");
        field.setAccessible(true);
        field.set(source, state);
    }

    private static RedoThreadState buildRedoThreadState(int threadId) {
        return RedoThreadState.builder()
                .thread()
                .threadId(threadId)
                .status("OPEN")
                .enabled("PUBLIC")
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

    private TransactionCommitConsumer getAccumulator(UnbufferedLogMinerStreamingChangeEventSource source) throws Exception {
        final var field = UnbufferedLogMinerStreamingChangeEventSource.class.getDeclaredField("accumulator");
        field.setAccessible(true);
        return (TransactionCommitConsumer) field.get(source);
    }

    private void setDatabaseOffset(UnbufferedLogMinerStreamingChangeEventSource source, ZoneOffset offset) throws Exception {
        final var field = UnbufferedLogMinerStreamingChangeEventSource.class.getDeclaredField("databaseOffset");
        field.setAccessible(true);
        field.set(source, offset);
    }

    private OracleDatabaseSchema createOracleDatabaseSchema() throws Exception {
        final Configuration configuration = getConfig().build();
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

        final Table table = Table.editor()
                .tableId(TableId.parse("ORCLPDB1.DEBEZIUM." + TABLE_NAME))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("DATA").create())
                .create();

        schema.refresh(table);
        return schema;
    }

    private OracleConnectionFactory createOracleConnectionFactory() throws Exception {
        final ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.next()).thenReturn(true);
        Mockito.when(rs.getFloat(1)).thenReturn(2.f);

        final PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(stmt.executeQuery()).thenReturn(rs);

        final Connection conn = Mockito.mock(Connection.class);
        Mockito.when(conn.prepareStatement(Mockito.any())).thenReturn(stmt);

        final OracleConnection connection = Mockito.mock(OracleConnection.class);
        Mockito.when(connection.connection(Mockito.anyBoolean())).thenReturn(conn);
        Mockito.when(connection.connection()).thenReturn(conn);
        Mockito.when(connection.getNationalCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.UTF8_CHARSET));
        Mockito.when(connection.getDatabaseCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.AL32UTF8_CHARSET));
        Mockito.when(connection.singleOptionalValue(anyString(), any())).thenReturn(BigInteger.TWO);
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
                .pollDispatchInterval(Duration.of(DEFAULT_POLL_DISPATCH_INTERVAL_MILLIS, ChronoUnit.MILLIS))
                .maxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                .maxQueueSize(DEFAULT_MAX_QUEUE_SIZE)
                .queueProvider(createDefaultQueueProvider(DEFAULT_MAX_QUEUE_SIZE))
                .build();

        return new LogMinerStreamingChangeEventSourceMetrics(taskContext, queue, null, connectorConfig, java.util.Collections::emptyList);
    }

    private static DefaultQueueProvider<DataChangeEvent> createDefaultQueueProvider(int maxQueueSize) {
        final DefaultQueueProvider<DataChangeEvent> provider = new DefaultQueueProvider<>();
        provider.configure(Map.of("max.queue.size", String.valueOf(maxQueueSize)));
        return provider;
    }

    // Helper class that permits exposing some protected methods for mocking
    protected static class UnbufferedStreamingChangeEventSource extends UnbufferedLogMinerStreamingChangeEventSource {

        private final ChangeEventSourceContext context;

        public UnbufferedStreamingChangeEventSource(OracleConnectorConfig connectorConfig,
                                                    OracleConnectionFactory connectionFactory,
                                                    EventDispatcher<OraclePartition, TableId> dispatcher,
                                                    OracleDatabaseSchema schema,
                                                    LogMinerStreamingChangeEventSourceMetrics metrics,
                                                    ChangeEventSourceContext context) {
            super(connectorConfig, connectionFactory, dispatcher, null, Clock.SYSTEM, schema, connectorConfig.getJdbcConfig(), metrics);
            this.context = context;
        }

        @Override
        protected ChangeEventSourceContext getContext() {
            // Necessary for mock purposes only
            return context;
        }

        @Override
        public void processEvent(LogMinerEventRow event) throws SQLException, InterruptedException {
            // Necessary for mock purposes only
            super.processEvent(event);
        }
    }
}
