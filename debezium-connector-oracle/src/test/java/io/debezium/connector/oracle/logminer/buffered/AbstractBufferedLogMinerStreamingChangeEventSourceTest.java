/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE;
import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE;
import static io.debezium.config.CommonConnectorConfig.DEFAULT_POLL_DISPATCH_INTERVAL_MILLIS;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
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
import io.debezium.connector.oracle.logminer.AbstractLogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.buffered.BufferedLogMinerStreamingChangeEventSource.ProcessResult;
import io.debezium.connector.oracle.logminer.events.CachedEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.parser.DmlParserException;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope.Operation;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

import ch.qos.logback.classic.Level;
import oracle.sql.CharacterSet;

/**
 * Abstract class implementation for all unit tests for {@link BufferedLogMinerStreamingChangeEventSource}.
 *
 * @author Chris Cranford
 */
public abstract class AbstractBufferedLogMinerStreamingChangeEventSourceTest extends AbstractAsyncEngineConnectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBufferedLogMinerStreamingChangeEventSourceTest.class);

    // The catalog of the mocked event rows and relational model, independent of the database the suite runs against
    private static final String CATALOG_NAME = "ORCLPDB1";
    private static final String TRANSACTION_ID_1 = "1234567890";
    private static final String TRANSACTION_ID_2 = "9876543210";
    private static final String TRANSACTION_ID_3 = "9880212345";
    private static final String PARTIAL_TXN_ID_FULL = "0e001c0012345678";
    private static final String PARTIAL_TXN_ID_PARTIAL = "0e001c00ffffffff";
    private static final String PARTIAL_TXN_ID_OTHER = "0f001d0087654321";
    private static final String PARTIAL_TXN_ID_SAME_PREFIX = "0e001c0087654321";

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

    protected abstract Configuration.Builder getConfig();

    protected boolean isTransactionAbandonmentSupported() {
        return true;
    }

    @Test
    void testCacheIsEmpty() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    void testCacheIsNotEmptyWhenTransactionIsAdded() throws Exception {
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
    void testCacheIsEmptyWhenTransactionIsCommitted() throws Exception {
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
    void testCacheIsEmptyWhenTransactionIsRolledBack() throws Exception {
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
    void testCacheIsNotEmptyWhenFirstTransactionIsRolledBack() throws Exception {
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
    void testCacheIsNotEmptyWhenSecondTransactionIsRolledBack() throws Exception {
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
    void testCalculateScnWhenTransactionIsCommitted() throws Exception {
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
    void testCalculateScnWhenFirstTransactionIsCommitted() throws Exception {
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
    void testCalculateScnWhenSecondTransactionIsCommitted() throws Exception {
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

            final ProcessResult result = mock.process(Scn.valueOf(100), Scn.valueOf(100), Scn.valueOf(200));
            assertThat(result.readStartScn()).isEqualTo(Scn.valueOf(100));
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
            Mockito.when(rs.getTimestamp(eq(4), any(Calendar.class))).thenReturn(Timestamp.valueOf(LocalDateTime.now()));
            Mockito.when(rs.getBytes(5)).thenReturn(new byte[]{ 0x0e, 0x00, 0x1c, 0x00, 0x12, 0x34, 0x56, 0x78 });
            Mockito.when(rs.getString(7)).thenReturn("ABC");
            Mockito.when(rs.getString(8)).thenReturn("DEBEZIUM");
            Mockito.when(rs.getString(10)).thenReturn("AAAAAAAAAAAAAAAAAB");

            final PreparedStatement ps = Mockito.mock(PreparedStatement.class);
            Mockito.when(ps.executeQuery()).thenReturn(rs);

            final BufferedStreamingChangeEventSource mock = Mockito.spy(source);
            Mockito.doReturn(ps).when(mock).createQueryStatement();

            final OracleConnection connection = connectionFactory.mainConnection();
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

            final ProcessResult result = mock.process(Scn.valueOf(100), Scn.valueOf(100), Scn.valueOf(200));
            assertThat(result.miningSessionStartScn()).isEqualTo(Scn.valueOf(100));
            assertThat(result.readStartScn()).isEqualTo(Scn.valueOf(101));
        }
    }

    @Test
    void testAbandonOneTransaction() throws Exception {
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
    void testAbandonTransactionHavingAnotherOne() throws Exception {
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

        connectionFactory = createOracleConnectionFactory(true);
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

        connectionFactory = createOracleConnectionFactory(true);
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

    @Test
    @FixFor("debezium/dbz#1960")
    public void testAbandonedTransactionDetailsListOnlyTablesWithLiveChanges() throws Exception {
        if (!isTransactionAbandonmentSupported()) {
            return;
        }

        final LogInterceptor logInterceptor = new LogInterceptor(BufferedLogMinerStreamingChangeEventSource.class);
        final ch.qos.logback.classic.Logger detailsLogger = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(BufferedLogMinerStreamingChangeEventSource.class.getName() + ".AbandonedDetails");
        final Level previousLevel = detailsLogger.getLevel();
        detailsLogger.setLevel(Level.DEBUG);
        try (var source = getChangeEventSource(getConfig().build())) {
            Mockito.when(offsetContext.getScn()).thenReturn(Scn.valueOf(1L));
            Mockito.when(offsetContext.getSnapshotScn()).thenReturn(Scn.NULL);

            final Instant changeTime = Instant.now().minus(24, ChronoUnit.HOURS);
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1, changeTime, "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'insert'"));
            // A change on a second table that is rolled back to a savepoint; only its undo marker stays in the cache
            source.processEvent(getUpdateLogMinerEventRow(3, TRANSACTION_ID_1, changeTime, "TEST_TABLE2", "AAAAAAAAAAAAAAAAAC", "'update'"));
            source.processEvent(getRollbackToSavepointLogMinerEventRow(4, TRANSACTION_ID_1, changeTime, "TEST_TABLE2", "AAAAAAAAAAAAAAAAAC", "'insert'"));
            source.abandonTransactions(Duration.ofHours(1L));

            assertThat(source.getTransactionCache().isEmpty()).isTrue();
            assertThat(logInterceptor.containsWarnMessage("1 tables [ORCLPDB1.DEBEZIUM.TEST_TABLE]) is being abandoned")).isTrue();
        }
        finally {
            detailsLogger.setLevel(previousLevel);
        }
    }

    @Test
    @FixFor({ "DBZ-1145", "debezium/dbz#2943" })
    public void testCacheIsEmptyWhenTransactionIsRolledBackWithPartialTransactionId() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, PARTIAL_TXN_ID_FULL));
            source.processEvent(getInsertLogMinerEventRow(2, PARTIAL_TXN_ID_FULL));

            assertThat(source.getTransactionCache().isEmpty()).isFalse();
            assertThat(source.getTransactionCache().containsTransaction(PARTIAL_TXN_ID_FULL)).isTrue();

            source.processEvent(getRollbackLogMinerEventRow(3, PARTIAL_TXN_ID_PARTIAL));

            assertThat(source.getTransactionCache().containsTransaction(PARTIAL_TXN_ID_FULL)).isFalse();
            // The rollback is attributed to the transaction it resolved to
            assertThat(metrics.getRolledBackTransactionIds()).contains(PARTIAL_TXN_ID_FULL);
        }
    }

    @Test
    @FixFor("DBZ-1145")
    public void testCacheIsNotEmptyWhenOnlyMatchingTransactionIsRolledBackWithPartialTransactionId() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, PARTIAL_TXN_ID_FULL));
            source.processEvent(getInsertLogMinerEventRow(2, PARTIAL_TXN_ID_FULL));
            source.processEvent(getStartLogMinerEventRow(3, PARTIAL_TXN_ID_OTHER));
            source.processEvent(getInsertLogMinerEventRow(4, PARTIAL_TXN_ID_OTHER));

            assertThat(source.getTransactionCache().containsTransaction(PARTIAL_TXN_ID_FULL)).isTrue();
            assertThat(source.getTransactionCache().containsTransaction(PARTIAL_TXN_ID_OTHER)).isTrue();

            source.processEvent(getRollbackLogMinerEventRow(5, PARTIAL_TXN_ID_PARTIAL));

            assertThat(source.getTransactionCache().containsTransaction(PARTIAL_TXN_ID_FULL)).isFalse();
            assertThat(source.getTransactionCache().containsTransaction(PARTIAL_TXN_ID_OTHER)).isTrue();
        }
    }

    @Test
    @FixFor("debezium/dbz#2576")
    public void testBatchDebugLoggingIncludesActiveTransactionMetadataOldestFirst() throws Exception {
        if (!isTransactionAbandonmentSupported()) {
            return;
        }

        final LogInterceptor logInterceptor = new LogInterceptor(BufferedLogMinerStreamingChangeEventSource.class);
        logInterceptor.setLoggerLevel(BufferedLogMinerStreamingChangeEventSource.class, Level.DEBUG);
        try (var source = getChangeEventSource(getConfig().build())) {
            final ResultSet rs = Mockito.mock(ResultSet.class);
            Mockito.when(rs.next()).thenReturn(false);

            final PreparedStatement ps = Mockito.mock(PreparedStatement.class);
            Mockito.when(ps.executeQuery()).thenReturn(rs);

            final BufferedStreamingChangeEventSource mock = Mockito.spy(source);
            Mockito.doReturn(ps).when(mock).createQueryStatement();

            // The second transaction is older by SCN and must be reported first
            final Instant firstStart = Instant.parse("2024-01-01T10:00:00Z");
            final Instant secondStart = Instant.parse("2024-01-01T09:00:00Z");
            mock.processEvent(getStartLogMinerEventRow(10, TRANSACTION_ID_1, firstStart));
            mock.processEvent(getInsertLogMinerEventRow(11, TRANSACTION_ID_1));
            mock.processEvent(getInsertLogMinerEventRow(12, TRANSACTION_ID_1));
            mock.processEvent(getStartLogMinerEventRow(5, TRANSACTION_ID_2, secondStart));
            mock.processEvent(getInsertLogMinerEventRow(6, TRANSACTION_ID_2));

            mock.process(Scn.valueOf(100), Scn.valueOf(100), Scn.valueOf(200));

            assertThat(logInterceptor.messageMatches("All active transactions: "
                    + TRANSACTION_ID_2 + " \\(startScn=5, changeTime=" + secondStart + ", userName=.*, clientId=.*, redoThread=\\d+, events=1\\), "
                    + TRANSACTION_ID_1 + " \\(startScn=10, changeTime=" + firstStart + ", userName=.*, clientId=.*, redoThread=\\d+, events=2\\)"))
                    .isTrue();
            assertThat(logInterceptor.containsMessage("All deferred transactions:")).isFalse();
        }
        finally {
            logInterceptor.setLoggerLevel(BufferedLogMinerStreamingChangeEventSource.class, null);
        }
    }

    @Test
    @FixFor("debezium/dbz#2577")
    public void testPendingTransactionsAreOrderedOldestFirstWithEventCounts() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            // Second transaction starts first by SCN but is added after the first to prove the ordering is by SCN
            source.processEvent(getStartLogMinerEventRow(10, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(11, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(12, TRANSACTION_ID_1));
            source.processEvent(getStartLogMinerEventRow(5, TRANSACTION_ID_2));
            source.processEvent(getInsertLogMinerEventRow(6, TRANSACTION_ID_2));

            final List<PendingTransaction> pending = source.getPendingTransactions();

            assertThat(pending).hasSize(2);
            assertThat(pending.get(0).transactionId()).isEqualTo(TRANSACTION_ID_2);
            assertThat(pending.get(0).startScn()).isEqualTo(Scn.valueOf(5));
            assertThat(pending.get(0).eventCount()).isEqualTo(1);
            assertThat(pending.get(0).deferred()).isFalse();
            assertThat(pending.get(1).transactionId()).isEqualTo(TRANSACTION_ID_1);
            assertThat(pending.get(1).startScn()).isEqualTo(Scn.valueOf(10));
            assertThat(pending.get(1).eventCount()).isEqualTo(2);
            assertThat(pending.get(1).deferred()).isFalse();

            source.processEvent(getCommitLogMinerEventRow(13, TRANSACTION_ID_1));
            source.processEvent(getCommitLogMinerEventRow(14, TRANSACTION_ID_2));

            assertThat(source.getPendingTransactions()).isEmpty();
        }
    }

    @Test
    @FixFor("DBZ-1145")
    public void testCacheIsNotEmptyWhenNoMatchingTransactionExistsForPartialTransactionId() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, PARTIAL_TXN_ID_OTHER));
            source.processEvent(getInsertLogMinerEventRow(2, PARTIAL_TXN_ID_OTHER));

            assertThat(source.getTransactionCache().containsTransaction(PARTIAL_TXN_ID_OTHER)).isTrue();

            source.processEvent(getRollbackLogMinerEventRow(3, PARTIAL_TXN_ID_PARTIAL));

            assertThat(source.getTransactionCache().containsTransaction(PARTIAL_TXN_ID_OTHER)).isTrue();
        }
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testPartialRollbackIsIgnoredWhenNoTransactionMatchesPrefix() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, PARTIAL_TXN_ID_OTHER));
            source.processEvent(getInsertLogMinerEventRow(2, PARTIAL_TXN_ID_OTHER, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'insert'"));
            source.processEvent(getUpdateLogMinerEventRow(3, PARTIAL_TXN_ID_OTHER, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'update'"));

            // The undo refers to a prefix that no cached transaction shares
            source.processEvent(getRollbackToSavepointLogMinerEventRow(4, PARTIAL_TXN_ID_PARTIAL, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'insert'"));

            assertThat(source.getTransactionCache().containsTransaction(PARTIAL_TXN_ID_PARTIAL)).isFalse();
            assertThat(source.getTransactionCache().getTransactionEventCount(source.getTransactionCache().getTransaction(PARTIAL_TXN_ID_OTHER))).isEqualTo(2);
            assertThat(metrics.getNumberOfPartialRollbackCount()).isZero();

            source.processEvent(getCommitLogMinerEventRow(5, PARTIAL_TXN_ID_OTHER));

            Mockito.verify(dispatcher, Mockito.times(1))
                    .dispatchDataChangeEvent(any(), any(), argThat(emitter -> emitter.getOperation() == Operation.CREATE));
            Mockito.verify(dispatcher, Mockito.times(1))
                    .dispatchDataChangeEvent(any(), any(), argThat(emitter -> emitter.getOperation() == Operation.UPDATE));
        }
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testPartialRollbackIsAppliedWhenExactlyOneTransactionMatchesPrefix() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, PARTIAL_TXN_ID_FULL));
            source.processEvent(getInsertLogMinerEventRow(2, PARTIAL_TXN_ID_FULL, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'insert'"));
            source.processEvent(getUpdateLogMinerEventRow(3, PARTIAL_TXN_ID_FULL, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'update'"));

            // The undo's sequence could not be resolved, but exactly one cached transaction shares its prefix
            source.processEvent(getRollbackToSavepointLogMinerEventRow(4, PARTIAL_TXN_ID_PARTIAL, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'insert'"));

            assertThat(source.getTransactionCache().containsTransaction(PARTIAL_TXN_ID_PARTIAL)).isFalse();
            assertThat(source.getTransactionCache().containsTransaction(PARTIAL_TXN_ID_FULL)).isTrue();
            assertThat(metrics.getNumberOfPartialRollbackCount()).isEqualTo(1);

            source.processEvent(getCommitLogMinerEventRow(5, PARTIAL_TXN_ID_FULL));

            Mockito.verify(dispatcher, Mockito.times(1))
                    .dispatchDataChangeEvent(any(), any(), argThat(emitter -> emitter.getOperation() == Operation.CREATE));
            Mockito.verify(dispatcher, Mockito.never())
                    .dispatchDataChangeEvent(any(), any(), argThat(emitter -> emitter.getOperation() == Operation.UPDATE));
        }
    }

    @Test
    @FixFor({ "debezium/dbz#1960", "debezium/dbz#2943" })
    public void testPartialRollbackFailsWhenMultipleTransactionsMatchPrefix() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, PARTIAL_TXN_ID_FULL));
            source.processEvent(getInsertLogMinerEventRow(2, PARTIAL_TXN_ID_FULL, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'insert'"));
            source.processEvent(getUpdateLogMinerEventRow(3, PARTIAL_TXN_ID_FULL, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'update'"));
            source.processEvent(getStartLogMinerEventRow(4, PARTIAL_TXN_ID_SAME_PREFIX));
            source.processEvent(getInsertLogMinerEventRow(5, PARTIAL_TXN_ID_SAME_PREFIX, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAC", "'insert'"));
            source.processEvent(getUpdateLogMinerEventRow(6, PARTIAL_TXN_ID_SAME_PREFIX, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAC", "'update'"));

            // Two transactions in flight share the undo's segment and slot, which Oracle never allows, so the
            // undo is refused rather than attributed to a guess
            assertThatThrownBy(() -> source.processEvent(
                    getRollbackToSavepointLogMinerEventRow(7, PARTIAL_TXN_ID_PARTIAL, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'insert'")))
                    .isInstanceOf(DebeziumException.class)
                    .hasMessageContaining(PARTIAL_TXN_ID_PARTIAL)
                    .hasMessageContaining(PARTIAL_TXN_ID_FULL)
                    .hasMessageContaining(PARTIAL_TXN_ID_SAME_PREFIX);

            assertThat(source.getTransactionCache().containsTransaction(PARTIAL_TXN_ID_PARTIAL)).isFalse();
            assertThat(source.getTransactionCache().getTransactionEventCount(source.getTransactionCache().getTransaction(PARTIAL_TXN_ID_FULL))).isEqualTo(2);
            assertThat(source.getTransactionCache().getTransactionEventCount(source.getTransactionCache().getTransaction(PARTIAL_TXN_ID_SAME_PREFIX))).isEqualTo(2);
            assertThat(metrics.getNumberOfPartialRollbackCount()).isZero();
        }
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testLastEnqueuedEventIsForgottenWhenPartialRollbackIsAppliedByPrefixAndTransactionCommits() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, PARTIAL_TXN_ID_FULL));
            source.processEvent(getInsertLogMinerEventRow(2, PARTIAL_TXN_ID_FULL, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'insert'"));
            source.processEvent(getUpdateLogMinerEventRow(3, PARTIAL_TXN_ID_FULL, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'update'"));
            source.processEvent(getRollbackToSavepointLogMinerEventRow(4, PARTIAL_TXN_ID_PARTIAL, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'insert'"));
            source.processEvent(getCommitLogMinerEventRow(5, PARTIAL_TXN_ID_FULL));

            // The undo was attributed to PARTIAL_TXN_ID_FULL, so committing it must leave nothing behind
            assertThat(source.getLastEnqueuedEventByTransactionId()).isEmpty();
        }
    }

    @Test
    @FixFor({ "DBZ-1914", "debezium/dbz#1960" })
    public void testSavepointRollbackIdempotence() throws Exception {
        final Configuration config = getConfig().build();
        try (var source = getChangeEventSource(config)) {
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'insert'"));
            source.processEvent(getUpdateLogMinerEventRow(3, TRANSACTION_ID_1, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'update'"));
            source.processEvent(getRollbackToSavepointLogMinerEventRow(4, TRANSACTION_ID_1, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'insert'"));
            // Simulate a new mining session
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'insert'"));
            source.processEvent(getUpdateLogMinerEventRow(3, TRANSACTION_ID_1, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'update'"));
            source.processEvent(getRollbackToSavepointLogMinerEventRow(4, TRANSACTION_ID_1, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'insert'"));
            source.processEvent(getCommitLogMinerEventRow(5, TRANSACTION_ID_1));
            Mockito.verify(dispatcher, Mockito.times(1))
                    .dispatchDataChangeEvent(any(), any(), argThat(emitter -> emitter.getOperation() == Operation.CREATE));
        }
    }

    @Test
    @FixFor("dbz#2049")
    void testEventFromPublicThreadIsNotSkipped() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.setCurrentRedoThreadState(buildRedoThreadState(1, "PUBLIC"));

            final LogMinerEventRow start = getStartLogMinerEventRow(1, TRANSACTION_ID_1);
            Mockito.when(start.getThread()).thenReturn(1);
            source.processEvent(start);

            final LogMinerEventRow insert = getInsertLogMinerEventRow(2, TRANSACTION_ID_1);
            Mockito.when(insert.getThread()).thenReturn(1);
            source.processEvent(insert);

            assertThat(source.getTransactionCache().isEmpty()).isFalse();
        }
    }

    @Test
    @FixFor("dbz#2049")
    void testEventFromPrivateThreadIsSkipped() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.setCurrentRedoThreadState(buildTwoThreadRedoThreadState(1, "PUBLIC", 2, "PRIVATE"));

            final LogMinerEventRow start1 = getStartLogMinerEventRow(1, TRANSACTION_ID_1);
            Mockito.when(start1.getThread()).thenReturn(2);
            source.processEvent(start1);

            final LogMinerEventRow insert1 = getInsertLogMinerEventRow(2, TRANSACTION_ID_1);
            Mockito.when(insert1.getThread()).thenReturn(2);
            source.processEvent(insert1);

            final LogMinerEventRow start2 = getStartLogMinerEventRow(3, TRANSACTION_ID_2);
            Mockito.when(start2.getThread()).thenReturn(1);
            source.processEvent(start2);

            final LogMinerEventRow insert2 = getInsertLogMinerEventRow(4, TRANSACTION_ID_2);
            Mockito.when(insert2.getThread()).thenReturn(1);
            source.processEvent(insert2);

            assertThat(source.getTransactionCache().isEmpty()).isFalse();
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_1)).isNull();
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_2)).isNotNull();
        }
    }

    @Test
    @FixFor("dbz#2049")
    void testRedoThreadTransitionFromPublicToPrivateLogsWarning() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerStreamingChangeEventSource.class);
        try (var source = getChangeEventSource(getConfig().build())) {
            source.setCurrentRedoThreadState(buildRedoThreadState(1, "PUBLIC"));
            source.detectRedoThreadTransitions(buildRedoThreadState(1, "PRIVATE"));

            assertThat(logInterceptor.containsWarnMessage(
                    "Redo Thread 1 just changed from PUBLIC to PRIVATE, which can lead to data loss and unexpected results."))
                    .isTrue();
        }
    }

    @Test
    @FixFor("dbz#2049")
    void testRedoThreadTransitionFromPrivateToPublicLogsWarning() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerStreamingChangeEventSource.class);
        try (var source = getChangeEventSource(getConfig().build())) {
            source.setCurrentRedoThreadState(buildRedoThreadState(1, "PRIVATE"));
            source.detectRedoThreadTransitions(buildRedoThreadState(1, "PUBLIC"));

            assertThat(logInterceptor.containsWarnMessage(
                    "Redo Thread 1 just changed from PRIVATE to PUBLIC, which can lead to unexpected results."))
                    .isTrue();
        }
    }

    @Test
    @FixFor("dbz#2777")
    void testTransactionEventsCountTracksCachedEvents() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            final LogMinerTransactionCache<Transaction> cache = source.getTransactionCache();
            assertTransactionEvents(cache, 0);

            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'insert'"));
            source.processEvent(getUpdateLogMinerEventRow(3, TRANSACTION_ID_1, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'update'"));
            source.processEvent(getStartLogMinerEventRow(4, TRANSACTION_ID_2));
            source.processEvent(getInsertLogMinerEventRow(5, TRANSACTION_ID_2));
            assertTransactionEvents(cache, 3);

            // The undo of the update removes the update and keeps the rollback event itself
            source.processEvent(getRollbackToSavepointLogMinerEventRow(6, TRANSACTION_ID_1, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'insert'"));
            assertTransactionEvents(cache, 3);

            source.processEvent(getCommitLogMinerEventRow(7, TRANSACTION_ID_1));
            assertTransactionEvents(cache, 1);

            source.processEvent(getRollbackLogMinerEventRow(8, TRANSACTION_ID_2));
            assertTransactionEvents(cache, 0);
        }
    }

    @Test
    @FixFor("debezium/dbz#2798")
    void shouldSkipEventForNonCapturedTableResolvedFromPrimaryOnPhysicalStandby() throws Exception {
        final Configuration config = getPhysicalStandbyConfig().build();
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final OracleConnection primary = connectionFactory.mainConnection();
        Mockito.when(primary.getTableIdByObjectId(CATALOG_NAME, 250282L, 250282L))
                .thenReturn(new TableId(CATALOG_NAME, "SYS", "MY_TABLE"));

        try (var source = getChangeEventSource(config)) {
            source.processEvent(getUnresolvableInsertLogMinerEventRow(2, TRANSACTION_ID_1, 250282L));

            Mockito.verify(primary).setSessionToPdb(connectorConfig.getPdbName());
            Mockito.verify(primary).getTableIdByObjectId(CATALOG_NAME, 250282L, 250282L);
            Mockito.verify(primary).resetSessionToCdb();
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_1)).isNull();
        }
    }

    @Test
    @FixFor("debezium/dbz#2798")
    void shouldFailWhenPrimaryCannotResolveObjectOnPhysicalStandby() throws Exception {
        // The mocked primary resolves nothing unless stubbed, which mirrors an object unknown to its catalog.
        try (var source = getChangeEventSource(getPhysicalStandbyConfig().build())) {
            assertThatThrownBy(() -> source.processEvent(getUnresolvableInsertLogMinerEventRow(2, TRANSACTION_ID_1, 250282L)))
                    .isInstanceOf(DebeziumException.class)
                    .hasMessageContaining("Failed to resolve a table for object id 250282");
        }
    }

    @Test
    @FixFor("debezium/dbz#2798")
    void shouldSkipEventWhenPrimaryCannotResolveObjectOnPhysicalStandbyAndFailureModeIsWarn() throws Exception {
        final Configuration config = getPhysicalStandbyConfig()
                .with(CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE, "warn")
                .build();
        final LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerStreamingChangeEventSource.class);

        try (var source = getChangeEventSource(config)) {
            source.processEvent(getUnresolvableInsertLogMinerEventRow(2, TRANSACTION_ID_1, 250282L));

            assertThat(logInterceptor.containsWarnMessage("Failed to resolve a table for object id 250282")).isTrue();
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_1)).isNull();
        }
    }

    @Test
    @FixFor("debezium/dbz#2798")
    void shouldNotQueryPrimaryForUnresolvableObjectWhenNotPhysicalStandby() throws Exception {
        final Configuration config = getConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.TEST_TABLE")
                .with(OracleConnectorConfig.LOG_MINING_STRATEGY, "hybrid")
                .build();
        final OracleConnection primary = connectionFactory.mainConnection();

        try (var source = getChangeEventSource(config)) {
            assertThatThrownBy(() -> source.processEvent(getUnresolvableInsertLogMinerEventRow(2, TRANSACTION_ID_1, 250282L)))
                    .isInstanceOf(DebeziumException.class);

            Mockito.verify(primary, Mockito.never()).getTableIdByObjectId(any(), Mockito.anyLong(), Mockito.anyLong());
        }
    }

    @Test
    @FixFor("debezium/dbz#2798")
    void shouldProcessEventForCapturedTableResolvedFromPrimaryOnPhysicalStandby() throws Exception {
        final Configuration config = getPhysicalStandbyConfig().build();
        final TableId resolvedTableId = new TableId(CATALOG_NAME, "DEBEZIUM", "TEST_TABLE");
        final OracleConnection primary = connectionFactory.mainConnection();
        // The stub only matches when the streaming source hands the event's catalog to the lookup.
        Mockito.when(primary.getTableIdByObjectId(CATALOG_NAME, 250282L, 250282L)).thenReturn(resolvedTableId);

        try (var source = getChangeEventSource(config)) {
            final LogMinerEventRow event = getUnresolvableInsertLogMinerEventRow(2, TRANSACTION_ID_1, 250282L);
            source.processEvent(event);

            // The identifier must match the relational model's table, including its catalog; otherwise the
            // event is treated as one for a newly discovered table and a schema change is dispatched for it.
            assertThat(event.getTableId()).isEqualTo(resolvedTableId);
            Mockito.verify(dispatcher, Mockito.never()).dispatchSchemaChangeEvent(any(), any(), any(), any());
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_1).getNumberOfEvents()).isEqualTo(1);
        }
    }

    @Test
    @FixFor("debezium/dbz#2798")
    void shouldQueryPrimaryOnceForRepeatedEventsOfNonCapturedObjectOnPhysicalStandby() throws Exception {
        final Configuration config = getPhysicalStandbyConfig().build();
        final OracleConnection primary = connectionFactory.mainConnection();
        Mockito.when(primary.getTableIdByObjectId(CATALOG_NAME, 250282L, 250282L))
                .thenReturn(new TableId(CATALOG_NAME, "SYS", "MY_TABLE"));

        try (var source = getChangeEventSource(config)) {
            source.processEvent(getUnresolvableInsertLogMinerEventRow(2, TRANSACTION_ID_1, 250282L));
            source.processEvent(getUnresolvableInsertLogMinerEventRow(3, TRANSACTION_ID_1, 250282L));

            Mockito.verify(primary, Mockito.times(1)).getTableIdByObjectId(CATALOG_NAME, 250282L, 250282L);
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_1)).isNull();
        }
    }

    @Test
    @FixFor("debezium/dbz#2798")
    void shouldQueryPrimaryAgainWhenDataObjectIdChangesOnPhysicalStandby() throws Exception {
        final Configuration config = getPhysicalStandbyConfig().build();
        final OracleConnection primary = connectionFactory.mainConnection();
        Mockito.when(primary.getTableIdByObjectId(eq(CATALOG_NAME), eq(250282L), Mockito.anyLong()))
                .thenReturn(new TableId(CATALOG_NAME, "SYS", "MY_TABLE"));

        try (var source = getChangeEventSource(config)) {
            // A truncate or a table move assigns a new data object id, which must invalidate the cached answer.
            source.processEvent(getUnresolvableInsertLogMinerEventRow(2, TRANSACTION_ID_1, 250282L, 250282L));
            source.processEvent(getUnresolvableInsertLogMinerEventRow(3, TRANSACTION_ID_1, 250282L, 250283L));

            Mockito.verify(primary).getTableIdByObjectId(CATALOG_NAME, 250282L, 250282L);
            Mockito.verify(primary).getTableIdByObjectId(CATALOG_NAME, 250282L, 250283L);
        }
    }

    @Test
    @FixFor("debezium/dbz#2798")
    void shouldCacheUnresolvableObjectFromPrimaryOnPhysicalStandbyWhenFailureModeIsWarn() throws Exception {
        final Configuration config = getPhysicalStandbyConfig()
                .with(CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE, "warn")
                .build();
        final OracleConnection primary = connectionFactory.mainConnection();

        try (var source = getChangeEventSource(config)) {
            source.processEvent(getUnresolvableInsertLogMinerEventRow(2, TRANSACTION_ID_1, 250282L));
            source.processEvent(getUnresolvableInsertLogMinerEventRow(3, TRANSACTION_ID_1, 250282L));

            Mockito.verify(primary, Mockito.times(1)).getTableIdByObjectId(CATALOG_NAME, 250282L, 250282L);
            assertThat(source.getTransactionCache().getTransaction(TRANSACTION_ID_1)).isNull();
        }
    }

    private Configuration.Builder getPhysicalStandbyConfig() {
        // An include list is required so that an object-id based table name is not treated as captured.
        return getConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.TEST_TABLE")
                .with(OracleConnectorConfig.CAPTURE_MODE, "physical_standby")
                .with(OracleConnectorConfig.LOG_MINING_ARCHIVE_LOG_ONLY_MODE, true)
                .with(OracleConnectorConfig.LOG_MINING_STRATEGY, "dictionary_from_file")
                .with(OracleConnectorConfig.LOG_MINING_PATH_DICTIONARY, "/opt/oracle/dictionary.ora");
    }

    /**
     * Creates an INSERT row for an object the mining dictionary cannot describe, which LogMiner names as
     * {@code UNKNOWN.OBJ# <id>}. The table identifier is held in a reference so that the mocked row observes
     * the identifier the streaming source resolves for it.
     */
    /**
     * A mocked row whose transaction id follows what the source sets, see
     * {@code BufferedLogMinerStreamingChangeEventSource#resolveTransactionId}.
     */
    private static LogMinerEventRow mockLogMinerEventRow() {
        final LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.doAnswer(invocation -> {
            Mockito.doReturn(invocation.getArgument(0)).when(row).getTransactionId();
            return null;
        }).when(row).setTransactionId(anyString());
        return row;
    }

    private LogMinerEventRow getUnresolvableInsertLogMinerEventRow(long scn, String transactionId, long objectId) {
        return getUnresolvableInsertLogMinerEventRow(scn, transactionId, objectId, objectId);
    }

    private LogMinerEventRow getUnresolvableInsertLogMinerEventRow(long scn, String transactionId, long objectId, long dataObjectId) {
        final AtomicReference<TableId> tableId = new AtomicReference<>(new TableId(CATALOG_NAME, "UNKNOWN", "OBJ# " + objectId));
        final LogMinerEventRow row = getInsertLogMinerEventRow(scn, transactionId, Instant.now(), "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'Test'");
        Mockito.when(row.getTableId()).thenAnswer(invocation -> tableId.get());
        Mockito.when(row.getTableName()).thenAnswer(invocation -> tableId.get().table());
        Mockito.doAnswer(invocation -> {
            tableId.set(invocation.getArgument(0));
            return null;
        }).when(row).setTableId(any());
        Mockito.when(row.getObjectId()).thenReturn(objectId);
        Mockito.when(row.getDataObjectId()).thenReturn(dataObjectId);
        return row;
    }

    private static void assertTransactionEvents(LogMinerTransactionCache<Transaction> cache, int expected) {
        // The running count must always match the events of the cached transactions
        final int cachedEvents = cache.streamTransactionsAndReturn(stream -> stream.mapToInt(cache::getTransactionEventCount).sum());
        assertThat(cachedEvents).isEqualTo(expected);
        assertThat(cache.getTransactionEvents()).isEqualTo(expected);
    }

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

        schema.refresh(table);

        // Typed columns: the field-name cache compares columns by type and the first table's are typeless
        Table secondTable = Table.editor()
                .tableId(TableId.parse("ORCLPDB1.DEBEZIUM.TEST_TABLE2"))
                .addColumn(Column.editor().name("ID").type("VARCHAR2(50)").create())
                .addColumn(Column.editor().name("DATA").type("VARCHAR2(50)").create())
                .create();

        schema.refresh(secondTable);

        // A table with a LOB column: with LOB support enabled, the commit consumer holds its rows for merging
        Table lobTable = Table.editor()
                .tableId(TableId.parse("ORCLPDB1.DEBEZIUM.LOB_TABLE"))
                .addColumn(Column.editor().name("ID").type("VARCHAR2(50)").create())
                .addColumn(Column.editor().name("DATA").type("CLOB").create())
                .create();

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
                .pollDispatchInterval(Duration.of(DEFAULT_POLL_DISPATCH_INTERVAL_MILLIS, ChronoUnit.MILLIS))
                .maxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                .maxQueueSize(DEFAULT_MAX_QUEUE_SIZE)
                .queueProvider(createDefaultQueueProvider(DEFAULT_MAX_QUEUE_SIZE))
                .build();

        return new LogMinerStreamingChangeEventSourceMetrics(taskContext, queue, null, connectorConfig, java.util.Collections::emptyList);
    }

    private static DefaultQueueProvider<DataChangeEvent> createDefaultQueueProvider(int maxQueueSize) {
        DefaultQueueProvider<DataChangeEvent> provider = new DefaultQueueProvider<>();
        provider.configure(java.util.Map.of("max.queue.size", String.valueOf(maxQueueSize)));
        return provider;
    }

    private LogMinerEventRow getStartLogMinerEventRow(long scn, String transactionId) {
        return getStartLogMinerEventRow(scn, transactionId, Instant.now());
    }

    private LogMinerEventRow getStartLogMinerEventRow(long scn, String transactionId, Instant changeTime) {
        LogMinerEventRow row = mockLogMinerEventRow();
        Mockito.when(row.getEventType()).thenReturn(EventType.START);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(Scn.valueOf(scn));
        Mockito.when(row.getChangeTime()).thenReturn(changeTime);
        return row;
    }

    private LogMinerEventRow getCommitLogMinerEventRow(long scn, String transactionId) {
        LogMinerEventRow row = mockLogMinerEventRow();
        Mockito.when(row.getEventType()).thenReturn(EventType.COMMIT);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(Scn.valueOf(scn));
        Mockito.when(row.getChangeTime()).thenReturn(Instant.now());
        return row;
    }

    private LogMinerEventRow getRollbackLogMinerEventRow(long scn, String transactionId) {
        LogMinerEventRow row = mockLogMinerEventRow();
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
        return getInsertLogMinerEventRow(scn, transactionId, changeTime, "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'Test'");
    }

    private LogMinerEventRow getInsertLogMinerEventRow(long scn, String transactionId, Instant changeTime, String tableName, String rowId, String dataValue) {
        LogMinerEventRow row = mockLogMinerEventRow();
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
        return row;
    }

    private LogMinerEventRow getUpdateLogMinerEventRow(long scn, String transactionId, Instant changeTime, String tableName, String rowId, String dataValue) {
        LogMinerEventRow row = mockLogMinerEventRow();
        Mockito.when(row.getEventType()).thenReturn(EventType.UPDATE);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(Scn.valueOf(scn));
        Mockito.when(row.getChangeTime()).thenReturn(changeTime);
        Mockito.when(row.getRowId()).thenReturn(rowId);
        Mockito.when(row.getOperation()).thenReturn("UPDATE");
        Mockito.when(row.getTableName()).thenReturn(tableName);
        Mockito.when(row.getTableId()).thenReturn(TableId.parse("ORCLPDB1.DEBEZIUM." + tableName));
        Mockito.when(row.getRedoSql()).thenReturn(
                "update \"DEBEZIUM\".\"%s\" set \"DATA\" = %s where \"ID\" = '1' and ROWID = '%s';".formatted(tableName, dataValue, rowId));
        Mockito.when(row.getRsId()).thenReturn("A.B.C");
        Mockito.when(row.getTablespaceName()).thenReturn("DEBEZIUM");
        Mockito.when(row.getUserName()).thenReturn(TestHelper.SCHEMA_USER);
        return row;
    }

    private LogMinerEventRow getRollbackToSavepointLogMinerEventRow(long scn, String transactionId, Instant changeTime, String tableName, String rowId,
                                                                    String dataValue) {
        LogMinerEventRow row = mockLogMinerEventRow();
        Mockito.when(row.getEventType()).thenReturn(EventType.UPDATE);
        Mockito.when(row.isRollbackFlag()).thenReturn(true);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(Scn.valueOf(scn));
        Mockito.when(row.getChangeTime()).thenReturn(changeTime);
        Mockito.when(row.getRowId()).thenReturn(rowId);
        Mockito.when(row.getOperation()).thenReturn("UPDATE");
        Mockito.when(row.getTableName()).thenReturn(tableName);
        Mockito.when(row.getTableId()).thenReturn(TableId.parse("ORCLPDB1.DEBEZIUM." + tableName));
        Mockito.when(row.getRedoSql()).thenReturn(
                "update \"DEBEZIUM\".\"%s\" set \"DATA\" = %s;".formatted(tableName, dataValue));
        Mockito.when(row.getRsId()).thenReturn("A.B.C");
        Mockito.when(row.getTablespaceName()).thenReturn("DEBEZIUM");
        Mockito.when(row.getUserName()).thenReturn(TestHelper.SCHEMA_USER);
        return row;
    }

    @Test
    @FixFor("debezium/dbz#2943")
    public void testParseFailureStopsProcessingByCommitWhenFailureModeIsFail() throws Exception {
        final Configuration config = getConfig()
                .with(CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE, "fail")
                .build();
        try (var source = getChangeEventSource(config)) {
            final Instant changeTime = Instant.now();
            // The row is parsed at commit, so the failure surfaces while the commit is being processed
            assertThatThrownBy(() -> {
                source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
                source.processEvent(getUnparsableInsertLogMinerEventRow(2, TRANSACTION_ID_1, changeTime));
                source.processEvent(getCommitLogMinerEventRow(3, TRANSACTION_ID_1));
            })
                    .isInstanceOf(DmlParserException.class)
                    .hasMessageContaining("DML statement couldn't be parsed");

            Mockito.verify(dispatcher, Mockito.never()).dispatchDataChangeEvent(any(), any(), any());
        }
    }

    @Test
    @FixFor("debezium/dbz#2943")
    public void testParseFailureSkipsEventWhenFailureModeIsWarn() throws Exception {
        final Configuration config = getConfig()
                .with(CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE, "warn")
                .build();
        final LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerStreamingChangeEventSource.class);
        try (var source = getChangeEventSource(config)) {
            final Instant changeTime = Instant.now();
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getUnparsableInsertLogMinerEventRow(2, TRANSACTION_ID_1, changeTime));
            source.processEvent(getInsertLogMinerEventRow(3, TRANSACTION_ID_1, changeTime));
            source.processEvent(getCommitLogMinerEventRow(4, TRANSACTION_ID_1));

            assertThat(logInterceptor.containsWarnMessage("cannot be parsed. This event will be ignored and skipped.")).isTrue();
            Mockito.verify(dispatcher, Mockito.times(1))
                    .dispatchDataChangeEvent(any(), any(), argThat(emitter -> emitter.getOperation() == Operation.CREATE));
            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    @FixFor("debezium/dbz#2943")
    public void testIncludedTableUnknownToSchemaIsResolvedByCommit() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            final BufferedStreamingChangeEventSource mock = Mockito.spy(source);

            // Typed columns, as the schema's field-name cache compares columns by type
            final TableId tableId = TableId.parse("ORCLPDB1.DEBEZIUM.NEW_TABLE");
            final Table table = Table.editor()
                    .tableId(tableId)
                    .addColumn(Column.editor().name("ID").type("VARCHAR2(50)").create())
                    .addColumn(Column.editor().name("DATA").type("VARCHAR2(50)").create())
                    .setPrimaryKeyNames("ID")
                    .create();

            // Stands in for fetching the definition from the database, which also registers it with the schema
            Mockito.doAnswer(invocation -> {
                schema.refresh(table);
                return table;
            }).when(mock).dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(tableId);

            final Instant changeTime = Instant.now();
            mock.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            mock.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1, changeTime, "NEW_TABLE", "AAAAAAAAAAAAAAAAAA", "'x'"));

            // Nothing about the table is needed until the transaction commits
            Mockito.verify(mock, Mockito.never()).dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(any());

            mock.processEvent(getCommitLogMinerEventRow(3, TRANSACTION_ID_1));

            Mockito.verify(mock, Mockito.times(1)).dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(tableId);
            Mockito.verify(dispatcher, Mockito.times(1))
                    .dispatchDataChangeEvent(any(), eq(tableId), argThat(emitter -> emitter.getOperation() == Operation.CREATE));
        }
    }

    @Test
    @FixFor("debezium/dbz#2943")
    public void testSchemaChangeIsAppliedInTransactionOrderAtCommit() throws Exception {
        final TableId tableId = TableId.parse("ORCLPDB1.DEBEZIUM.TEST_TABLE");
        try (var source = getChangeEventSource(getConfig().build())) {
            final Instant changeTime = Instant.now();
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1, changeTime));
            source.processEvent(getDdlLogMinerEventRow(3, TRANSACTION_ID_1, changeTime, "TEST_TABLE",
                    "alter table \"DEBEZIUM\".\"TEST_TABLE\" add (\"NAME\" varchar2(50));"));
            source.processEvent(getInsertLogMinerEventRow(4, TRANSACTION_ID_1, changeTime));

            // The schema change waits in its transaction
            Mockito.verify(dispatcher, Mockito.never()).dispatchSchemaChangeEvent(any(), any(), any(), any());
            Mockito.verify(dispatcher, Mockito.never()).dispatchDataChangeEvent(any(), any(), any());

            source.processEvent(getCommitLogMinerEventRow(5, TRANSACTION_ID_1));

            final InOrder inOrder = Mockito.inOrder(dispatcher);
            // The schema change sits between the two data changes, where the transaction put it
            inOrder.verify(dispatcher).dispatchDataChangeEvent(any(), eq(tableId), argThat(emitter -> emitter.getOperation() == Operation.CREATE));
            inOrder.verify(dispatcher).dispatchSchemaChangeEvent(any(), any(), eq(tableId), any());
            inOrder.verify(dispatcher).dispatchDataChangeEvent(any(), eq(tableId), argThat(emitter -> emitter.getOperation() == Operation.CREATE));
            Mockito.verify(dispatcher, Mockito.times(1)).dispatchSchemaChangeEvent(any(), any(), any(), any());
            assertThat(source.getTransactionCache().isEmpty()).isTrue();
            assertThat(metrics.getBatchMetrics().getSchemaChangeCount()).isEqualTo(1);
        }
    }

    @Test
    @FixFor("debezium/dbz#2943")
    public void testSchemaChangeIsAppliedWhenCommitHasNoTransactionSequence() throws Exception {
        final TableId tableId = TableId.parse("ORCLPDB1.DEBEZIUM.TEST_TABLE");
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, PARTIAL_TXN_ID_FULL));
            source.processEvent(getDdlLogMinerEventRow(2, PARTIAL_TXN_ID_FULL, Instant.now(), "TEST_TABLE",
                    "alter table \"DEBEZIUM\".\"TEST_TABLE\" add (\"NAME\" varchar2(50));"));

            // The commit was read by a new mining session, which could not resolve the transaction's sequence
            source.processEvent(getCommitLogMinerEventRow(3, PARTIAL_TXN_ID_PARTIAL));

            Mockito.verify(dispatcher, Mockito.times(1)).dispatchSchemaChangeEvent(any(), any(), eq(tableId), any());
            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    @FixFor("debezium/dbz#2943")
    public void testSchemaChangeWithDataChangesIsAppliedWhenCommitHasNoTransactionSequence() throws Exception {
        final TableId tableId = TableId.parse("ORCLPDB1.DEBEZIUM.TEST_TABLE");
        try (var source = getChangeEventSource(getConfig().build())) {
            // A schema change followed by data changes in one transaction, as CREATE TABLE AS SELECT produces
            final Instant changeTime = Instant.now();
            source.processEvent(getStartLogMinerEventRow(1, PARTIAL_TXN_ID_FULL));
            source.processEvent(getDdlLogMinerEventRow(2, PARTIAL_TXN_ID_FULL, changeTime, "TEST_TABLE",
                    "alter table \"DEBEZIUM\".\"TEST_TABLE\" add (\"NAME\" varchar2(50));"));
            source.processEvent(getInsertLogMinerEventRow(3, PARTIAL_TXN_ID_FULL, changeTime, "TEST_TABLE", "AAAAAAAAAAAAAAAAAB", "'first'"));
            // The session restarted here, so the rest of the transaction carries no sequence
            source.processEvent(getInsertLogMinerEventRow(4, PARTIAL_TXN_ID_PARTIAL, changeTime, "TEST_TABLE", "AAAAAAAAAAAAAAAAAC", "'second'"));
            source.processEvent(getCommitLogMinerEventRow(5, PARTIAL_TXN_ID_PARTIAL));

            final InOrder inOrder = Mockito.inOrder(dispatcher);
            inOrder.verify(dispatcher).dispatchSchemaChangeEvent(any(), any(), eq(tableId), any());
            inOrder.verify(dispatcher, Mockito.times(2)).dispatchDataChangeEvent(any(), eq(tableId), argThat(emitter -> emitter.getOperation() == Operation.CREATE));
            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    @FixFor("debezium/dbz#2943")
    public void testDataChangeWithNoTransactionSequenceJoinsItsTransaction() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, PARTIAL_TXN_ID_FULL));
            source.processEvent(getInsertLogMinerEventRow(2, PARTIAL_TXN_ID_FULL));
            source.processEvent(getInsertLogMinerEventRow(3, PARTIAL_TXN_ID_PARTIAL));

            // The change joins the transaction in flight rather than starting one of its own
            assertThat(source.getTransactionCache().getTransactionCount()).isEqualTo(1);
            assertThat(source.getTransactionCache().containsTransaction(PARTIAL_TXN_ID_PARTIAL)).isFalse();
            assertThat(source.getTransactionCache().getTransactionEventCount(source.getTransactionCache().getTransaction(PARTIAL_TXN_ID_FULL))).isEqualTo(2);

            source.processEvent(getCommitLogMinerEventRow(4, PARTIAL_TXN_ID_PARTIAL));

            Mockito.verify(dispatcher, Mockito.times(2)).dispatchDataChangeEvent(any(), any(), argThat(emitter -> emitter.getOperation() == Operation.CREATE));
            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    @FixFor("debezium/dbz#2943")
    public void testCommitWithNoTransactionSequenceFailsWhenMultipleTransactionsMatchPrefix() throws Exception {
        try (var source = getChangeEventSource(getConfig().build())) {
            source.processEvent(getStartLogMinerEventRow(1, PARTIAL_TXN_ID_FULL));
            source.processEvent(getInsertLogMinerEventRow(2, PARTIAL_TXN_ID_FULL));
            source.processEvent(getStartLogMinerEventRow(3, PARTIAL_TXN_ID_SAME_PREFIX));
            source.processEvent(getInsertLogMinerEventRow(4, PARTIAL_TXN_ID_SAME_PREFIX));

            assertThatThrownBy(() -> source.processEvent(getCommitLogMinerEventRow(5, PARTIAL_TXN_ID_PARTIAL)))
                    .isInstanceOf(DebeziumException.class)
                    .hasMessageContaining(PARTIAL_TXN_ID_FULL)
                    .hasMessageContaining(PARTIAL_TXN_ID_SAME_PREFIX);

            Mockito.verify(dispatcher, Mockito.never()).dispatchDataChangeEvent(any(), any(), any());
            assertThat(source.getTransactionCache().getTransactionCount()).isEqualTo(2);
        }
    }

    @Test
    @FixFor("debezium/dbz#2943")
    public void testSchemaChangeInDeferredTransactionIsAppliedAtCommit() throws Exception {
        final Configuration config = getConfig()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_DEFERRED_TRANSACTION_START, true)
                .build();
        final TableId tableId = TableId.parse("ORCLPDB1.DEBEZIUM.TEST_TABLE");
        try (var source = getChangeEventSource(config)) {
            final Instant changeTime = Instant.now();
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            // The START was deferred; the DDL is the transaction's first stored event and promotes it
            source.processEvent(getDdlLogMinerEventRow(2, TRANSACTION_ID_1, changeTime, "TEST_TABLE",
                    "alter table \"DEBEZIUM\".\"TEST_TABLE\" add (\"NAME\" varchar2(50));"));
            assertThat(source.getTransactionCache().getTransactionCount()).isEqualTo(1);
            Mockito.verify(dispatcher, Mockito.never()).dispatchSchemaChangeEvent(any(), any(), any(), any());

            source.processEvent(getCommitLogMinerEventRow(3, TRANSACTION_ID_1));

            Mockito.verify(dispatcher, Mockito.times(1)).dispatchSchemaChangeEvent(any(), any(), eq(tableId), any());
            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    @FixFor("debezium/dbz#2943")
    public void testSchemaChangeAlreadySentIsNotReappliedWhenTransactionIsReplayed() throws Exception {
        // The connector stopped after the schema change was applied and recorded as event 1 of the
        // transaction, but before the commit reached the offsets: the restart re-mines the transaction
        Mockito.when(offsetContext.getTransactionId()).thenReturn(TRANSACTION_ID_1);
        Mockito.when(offsetContext.getTransactionSequence()).thenReturn(1L);

        final TableId tableId = TableId.parse("ORCLPDB1.DEBEZIUM.TEST_TABLE");
        try (var source = getChangeEventSource(getConfig().build())) {
            final Instant changeTime = Instant.now();
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getDdlLogMinerEventRow(2, TRANSACTION_ID_1, changeTime, "TEST_TABLE",
                    "alter table \"DEBEZIUM\".\"TEST_TABLE\" add (\"NAME\" varchar2(50));"));
            source.processEvent(getInsertLogMinerEventRow(3, TRANSACTION_ID_1, changeTime));
            source.processEvent(getCommitLogMinerEventRow(4, TRANSACTION_ID_1));

            Mockito.verify(dispatcher, Mockito.never()).dispatchSchemaChangeEvent(any(), any(), any(), any());
            Mockito.verify(dispatcher, Mockito.times(1))
                    .dispatchDataChangeEvent(any(), eq(tableId), argThat(emitter -> emitter.getOperation() == Operation.CREATE));
            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    @FixFor("debezium/dbz#2943")
    public void testSchemaChangeFlushesRowsHeldForLobMerging() throws Exception {
        final Configuration config = getConfig().with(OracleConnectorConfig.LOB_ENABLED, true).build();
        final TableId tableId = TableId.parse("ORCLPDB1.DEBEZIUM.LOB_TABLE");
        try (var source = getChangeEventSource(config)) {
            final Instant changeTime = Instant.now();
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            // Held by the commit consumer until a LOB chain is merged or the transaction ends
            source.processEvent(getInsertLogMinerEventRow(2, TRANSACTION_ID_1, changeTime, "LOB_TABLE", "AAAAAAAAAAAAAAAAAB", "'x'"));
            source.processEvent(getDdlLogMinerEventRow(3, TRANSACTION_ID_1, changeTime, "LOB_TABLE",
                    "alter table \"DEBEZIUM\".\"LOB_TABLE\" add (\"NAME\" varchar2(50));"));
            source.processEvent(getInsertLogMinerEventRow(4, TRANSACTION_ID_1, changeTime, "LOB_TABLE", "AAAAAAAAAAAAAAAAAC", "'y'"));
            source.processEvent(getCommitLogMinerEventRow(5, TRANSACTION_ID_1));

            final InOrder inOrder = Mockito.inOrder(dispatcher);
            // The held row is dispatched before the schema change; nothing is merged across it
            inOrder.verify(dispatcher).dispatchDataChangeEvent(any(), eq(tableId), argThat(emitter -> emitter.getOperation() == Operation.CREATE));
            inOrder.verify(dispatcher).dispatchSchemaChangeEvent(any(), any(), eq(tableId), any());
            inOrder.verify(dispatcher).dispatchDataChangeEvent(any(), eq(tableId), argThat(emitter -> emitter.getOperation() == Operation.CREATE));
            Mockito.verify(dispatcher, Mockito.times(1)).dispatchSchemaChangeEvent(any(), any(), any(), any());
            Mockito.verify(dispatcher, Mockito.times(2)).dispatchDataChangeEvent(any(), any(), any());
        }
    }

    @Test
    @FixFor("debezium/dbz#2943")
    public void testTruncateIsEmittedAtCommit() throws Exception {
        applySchemaChangesWhenDispatched();
        final TableId tableId = TableId.parse("ORCLPDB1.DEBEZIUM.TEST_TABLE");
        try (var source = getChangeEventSource(getConfig().build())) {
            final Instant changeTime = Instant.now();
            source.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            source.processEvent(getDdlLogMinerEventRow(2, TRANSACTION_ID_1, changeTime, "TEST_TABLE",
                    "truncate table \"DEBEZIUM\".\"TEST_TABLE\";"));

            // In both modes the truncate's change event belongs to the transaction
            Mockito.verify(dispatcher, Mockito.never()).dispatchDataChangeEvent(any(), any(), any());

            source.processEvent(getCommitLogMinerEventRow(3, TRANSACTION_ID_1));

            Mockito.verify(dispatcher, Mockito.times(1))
                    .dispatchDataChangeEvent(any(), eq(tableId), argThat(emitter -> emitter.getOperation() == Operation.TRUNCATE));
            assertThat(source.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    @FixFor({ "DBZ-6650", "debezium/dbz#2943" })
    public void testTruncateOfIncludedTableUnknownToSchemaIsEmittedAtCommit() throws Exception {
        applySchemaChangesWhenDispatched();
        final TableId tableId = TableId.parse("ORCLPDB1.DEBEZIUM.NEW_TABLE");
        final Table table = Table.editor()
                .tableId(tableId)
                .addColumn(Column.editor().name("ID").type("VARCHAR2(50)").create())
                .addColumn(Column.editor().name("DATA").type("VARCHAR2(50)").create())
                .setPrimaryKeyNames("ID")
                .create();
        try (var source = getChangeEventSource(getConfig().build())) {
            final BufferedStreamingChangeEventSource mock = Mockito.spy(source);

            // The table was not captured before, so the truncate fetches its definition as a data change would
            Mockito.doAnswer(invocation -> {
                schema.refresh(table);
                return table;
            }).when(mock).dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(tableId);

            final Instant changeTime = Instant.now();
            mock.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            mock.processEvent(getDdlLogMinerEventRow(2, TRANSACTION_ID_1, changeTime, "NEW_TABLE",
                    "truncate table \"DEBEZIUM\".\"NEW_TABLE\";"));

            Mockito.verify(mock, Mockito.never()).dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(any());
            Mockito.verify(dispatcher, Mockito.never()).dispatchDataChangeEvent(any(), any(), any());

            mock.processEvent(getCommitLogMinerEventRow(3, TRANSACTION_ID_1));

            Mockito.verify(mock, Mockito.times(1)).dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(tableId);
            Mockito.verify(dispatcher, Mockito.times(1))
                    .dispatchDataChangeEvent(any(), eq(tableId), argThat(emitter -> emitter.getOperation() == Operation.TRUNCATE));
            assertThat(mock.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    @FixFor("debezium/dbz#801")
    public void testCreateTableAsSelectIsResolvedAsOfCommit() throws Exception {
        applySchemaChangesWhenDispatched();
        final TableId tableId = TableId.parse("ORCLPDB1.DEBEZIUM.NEW_TABLE");
        try (var source = getChangeEventSource(getConfig().build())) {
            final BufferedStreamingChangeEventSource mock = Mockito.spy(source);

            // Stands in for reading the definition through DBMS_FLASHBACK at the commit SCN
            Mockito.doReturn("CREATE TABLE \"DEBEZIUM\".\"NEW_TABLE\" (\"ID\" VARCHAR2(50), \"DATA\" VARCHAR2(50));")
                    .when(mock).getTableDdlAtScn(eq(tableId), any());

            final Instant changeTime = Instant.now();
            mock.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            mock.processEvent(getDdlLogMinerEventRow(2, TRANSACTION_ID_1, changeTime, "NEW_TABLE",
                    "create table \"DEBEZIUM\".\"NEW_TABLE\" as select \"ID\", \"DATA\" from \"DEBEZIUM\".\"TEST_TABLE\";"));
            mock.processEvent(getInsertLogMinerEventRow(3, TRANSACTION_ID_1, changeTime, "NEW_TABLE", "AAAAAAAAAAAAAAAAAB", "'x'"));

            Mockito.verify(mock, Mockito.never()).getTableDdlAtScn(any(), any());
            assertThat(schema.tableFor(tableId)).isNull();

            mock.processEvent(getCommitLogMinerEventRow(4, TRANSACTION_ID_1));

            Mockito.verify(mock, Mockito.times(1)).getTableDdlAtScn(tableId, Scn.valueOf(4));
            Mockito.verify(mock, Mockito.never()).dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(any());
            Mockito.verify(dispatcher, Mockito.times(1)).dispatchSchemaChangeEvent(any(), any(), eq(tableId), any());
            Mockito.verify(dispatcher, Mockito.times(1))
                    .dispatchDataChangeEvent(any(), eq(tableId), argThat(emitter -> emitter.getOperation() == Operation.CREATE));

            final Table table = schema.tableFor(tableId);
            assertThat(table).isNotNull();
            assertThat(table.retrieveColumnNames()).containsExactly("ID", "DATA");
        }
    }

    @Test
    @FixFor("debezium/dbz#801")
    public void testCreateTableAsSelectFallsBackToCurrentDefinitionWhenUnresolved() throws Exception {
        applySchemaChangesWhenDispatched();
        final TableId tableId = TableId.parse("ORCLPDB1.DEBEZIUM.NEW_TABLE");
        final Table table = Table.editor()
                .tableId(tableId)
                .addColumn(Column.editor().name("ID").type("VARCHAR2(50)").create())
                .addColumn(Column.editor().name("DATA").type("VARCHAR2(50)").create())
                .create();
        try (var source = getChangeEventSource(getConfig().build())) {
            final BufferedStreamingChangeEventSource mock = Mockito.spy(source);

            // The definition could not be read as of the commit and the failure handling mode skipped the schema change
            Mockito.doReturn(null).when(mock).getTableDdlAtScn(eq(tableId), any());
            // The transaction's rows then fetch the current definition, as any included but unknown table does
            Mockito.doAnswer(invocation -> {
                schema.refresh(table);
                return table;
            }).when(mock).dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(tableId);

            final Instant changeTime = Instant.now();
            mock.processEvent(getStartLogMinerEventRow(1, TRANSACTION_ID_1));
            mock.processEvent(getDdlLogMinerEventRow(2, TRANSACTION_ID_1, changeTime, "NEW_TABLE",
                    "create table \"DEBEZIUM\".\"NEW_TABLE\" as select \"ID\", \"DATA\" from \"DEBEZIUM\".\"TEST_TABLE\";"));
            mock.processEvent(getInsertLogMinerEventRow(3, TRANSACTION_ID_1, changeTime, "NEW_TABLE", "AAAAAAAAAAAAAAAAAB", "'x'"));
            mock.processEvent(getCommitLogMinerEventRow(4, TRANSACTION_ID_1));

            Mockito.verify(mock, Mockito.times(1)).getTableDdlAtScn(tableId, Scn.valueOf(4));
            Mockito.verify(mock, Mockito.times(1)).dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(tableId);
            Mockito.verify(dispatcher, Mockito.times(1))
                    .dispatchDataChangeEvent(any(), eq(tableId), argThat(emitter -> emitter.getOperation() == Operation.CREATE));
        }
    }

    /**
     * Runs each dispatched schema change's emitter, so the parser applies the statement to the schema's tables;
     * the mocked dispatcher would otherwise leave the schema untouched.
     */
    private void applySchemaChangesWhenDispatched() throws InterruptedException {
        // The emitted schema change events carry the offset's source info, which the mocked offsets lack
        Mockito.when(offsetContext.getSourceInfo()).thenReturn(new Struct(SchemaBuilder.struct().name("source").build()));
        Mockito.doAnswer(invocation -> {
            final SchemaChangeEventEmitter emitter = invocation.getArgument(3);
            emitter.emitSchemaChangeEvent(event -> {
            });
            return null;
        }).when(dispatcher).dispatchSchemaChangeEvent(any(), any(), any(), any());
    }

    private LogMinerEventRow getUnparsableInsertLogMinerEventRow(long scn, String transactionId, Instant changeTime) {
        final LogMinerEventRow row = getInsertLogMinerEventRow(scn, transactionId, changeTime);
        Mockito.when(row.getRedoSql()).thenReturn("insert into \"DEBEZIUM\".\"TEST_TABLE\" values nonsense");
        return row;
    }

    private LogMinerEventRow getDdlLogMinerEventRow(long scn, String transactionId, Instant changeTime, String tableName, String ddl) {
        LogMinerEventRow row = mockLogMinerEventRow();
        Mockito.when(row.getEventType()).thenReturn(EventType.DDL);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(Scn.valueOf(scn));
        Mockito.when(row.getChangeTime()).thenReturn(changeTime);
        Mockito.when(row.getRowId()).thenReturn("AAAAAAAAAAAAAAAAAA");
        Mockito.when(row.getOperation()).thenReturn("DDL");
        Mockito.when(row.getTableName()).thenReturn(tableName);
        Mockito.when(row.getTableId()).thenReturn(TableId.parse("ORCLPDB1.DEBEZIUM." + tableName));
        Mockito.when(row.getRedoSql()).thenReturn(ddl);
        Mockito.when(row.getRsId()).thenReturn("A.B.C");
        Mockito.when(row.getTablespaceName()).thenReturn("DEBEZIUM");
        Mockito.when(row.getUserName()).thenReturn(TestHelper.SCHEMA_USER);
        Mockito.when(row.getThread()).thenReturn(1);
        return row;
    }

    protected BufferedStreamingChangeEventSource getChangeEventSource(Configuration config) throws InterruptedException {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        assertThat(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error)).isTrue();

        final BufferedStreamingChangeEventSource source = new BufferedStreamingChangeEventSource(
                connectorConfig,
                connectionFactory,
                dispatcher,
                schema,
                metrics,
                context,
                partition,
                offsetContext);

        source.init(offsetContext);

        try {
            source.setCurrentRedoThreadState(buildRedoThreadState(1, "PUBLIC"));
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to set redo thread state", e);
        }

        return source;
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

    private static RedoThreadState buildTwoThreadRedoThreadState(int threadId1, String enabled1, int threadId2, String enabled2) {
        return RedoThreadState.builder()
                .thread()
                .threadId(threadId1)
                .status("OPEN")
                .enabled(enabled1)
                .logGroups(2L)
                .instanceName("ORCLCDB1")
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
                .thread()
                .threadId(threadId2)
                .status("OPEN")
                .enabled(enabled2)
                .logGroups(2L)
                .instanceName("ORCLCDB2")
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

    // Helper class that permits exposing some protected methods for mocking
    protected static class BufferedStreamingChangeEventSource extends BufferedLogMinerStreamingChangeEventSource {

        private final ChangeEventSourceContext context;
        private final OraclePartition partition;

        public BufferedStreamingChangeEventSource(
                                                  OracleConnectorConfig connectorConfig,
                                                  OracleConnectionFactory connectionFactory,
                                                  EventDispatcher<OraclePartition, TableId> dispatcher,
                                                  OracleDatabaseSchema schema,
                                                  LogMinerStreamingChangeEventSourceMetrics metrics,
                                                  ChangeEventSourceContext context,
                                                  OraclePartition partition,
                                                  OracleOffsetContext offsetContext) {
            super(connectorConfig, connectionFactory, dispatcher, null, Clock.SYSTEM, schema, connectorConfig.getJdbcConfig(), metrics);
            this.context = context;
            this.partition = partition;
        }

        @Override
        protected ChangeEventSourceContext getContext() {
            // Necessary for mock purposes only
            return context;
        }

        @Override
        protected OraclePartition getPartition() {
            // Set by execute(), which the tests do not run; the schema change emitters read it
            return partition;
        }

        @Override
        public Table dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(TableId tableId) throws SQLException, InterruptedException {
            // Necessary for mock purposes only
            return super.dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(tableId);
        }

        @Override
        public String getTableDdlAtScn(TableId tableId, Scn scn) {
            return super.getTableDdlAtScn(tableId, scn);
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

        @SuppressWarnings("unchecked")
        public Map<String, CachedEvent> getLastEnqueuedEventByTransactionId() throws Exception {
            var field = AbstractLogMinerTransactionCache.class.getDeclaredField("lastEnqueuedEventByTransactionId");
            field.setAccessible(true);
            return (Map<String, CachedEvent>) field.get(this.getTransactionCache());
        }

        @Override
        public void detectRedoThreadTransitions(RedoThreadState newRedoThreadState) {
            super.detectRedoThreadTransitions(newRedoThreadState);
        }
    }
}
