/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE;
import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE;
import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntryImpl;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.connector.oracle.xstream.LcrPosition;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * @author Andrey Pustovetov
 */
@SkipWhenAdapterNameIsNot(value = AdapterName.LOGMINER)
public class TransactionalBufferTest {

    private static final String SERVER_NAME = "serverX";
    private static final String TRANSACTION_ID = "transaction";
    private static final String OTHER_TRANSACTION_ID = "other_transaction";
    private static final String MESSAGE = "OK";
    private static final Scn SCN_ONE = new Scn(BigInteger.ONE);
    private static final Scn SCN = SCN_ONE;
    private static final Scn OTHER_SCN = Scn.valueOf(10L);
    private static final Timestamp TIMESTAMP = new Timestamp(System.currentTimeMillis());
    private static final TableId TABLE_ID = new TableId(TestHelper.SERVER_NAME, "DEBEZIUM", "TEST");
    private static final String ROW_ID = "AAABCD871DFAA";
    private static final String OTHER_ROW_ID = "BAABCD871DFAA";
    private static final LogMinerDmlEntry DML_ENTRY = new LogMinerDmlEntryImpl(Envelope.Operation.CREATE, Collections.emptyList(), Collections.emptyList());

    private static final Configuration config = new Configuration() {
        @Override
        public Set<String> keys() {
            return Collections.emptySet();
        }

        @Override
        public String getString(String key) {
            return null;
        }
    };
    private static final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
    private static OracleOffsetContext offsetContext;

    private OracleTaskContext taskContext;
    private ErrorHandler errorHandler;
    private TransactionalBuffer transactionalBuffer;
    private OracleStreamingChangeEventSourceMetrics streamingMetrics;
    private OracleDatabaseSchema schema;
    private Clock clock;
    private EventDispatcher<TableId> dispatcher;

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    @Before
    public void before() {
        ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(Duration.of(DEFAULT_MAX_QUEUE_SIZE, ChronoUnit.MILLIS))
                .maxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                .maxQueueSize(DEFAULT_MAX_QUEUE_SIZE)
                .build();
        errorHandler = new ErrorHandler(OracleConnector.class, SERVER_NAME, queue);

        taskContext = mock(OracleTaskContext.class);
        Mockito.when(taskContext.getConnectorName()).thenReturn("connector name");
        Mockito.when(taskContext.getConnectorType()).thenReturn("connector type");

        // noinspection unchecked
        dispatcher = (EventDispatcher<TableId>) mock(EventDispatcher.class);
        schema = mock(OracleDatabaseSchema.class);
        clock = mock(Clock.class);

        streamingMetrics = new OracleStreamingChangeEventSourceMetrics(taskContext, queue, null, connectorConfig);
        transactionalBuffer = new TransactionalBuffer(schema, clock, errorHandler, streamingMetrics);
    }

    @After
    public void after() throws InterruptedException {
        transactionalBuffer.close();
    }

    @Test
    public void testIsEmpty() {
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(true);
    }

    @Test
    public void testIsNotEmptyWhenTransactionIsRegistered() {
        registerDmlOperation(TRANSACTION_ID, SCN, ROW_ID);
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(false);
    }

    @Test
    public void testIsEmptyWhenTransactionIsCommitted() throws Exception {
        registerDmlOperation(TRANSACTION_ID, SCN, ROW_ID);
        offsetContext = new OracleOffsetContext(connectorConfig, SCN, SCN, (LcrPosition) null, false, true, new TransactionContext());
        transactionalBuffer.commit(TRANSACTION_ID, SCN.add(SCN_ONE), offsetContext, TIMESTAMP, () -> true, MESSAGE, dispatcher);
        assertThat(transactionalBuffer.isEmpty()).isTrue();
    }

    @Test
    public void testIsEmptyWhenTransactionIsRolledBack() throws Exception {
        registerDmlOperation(TRANSACTION_ID, SCN, ROW_ID);
        transactionalBuffer.rollback(TRANSACTION_ID, "");
        assertThat(transactionalBuffer.isEmpty()).isTrue();
    }

    @Test
    public void testNonEmptyFirstTransactionIsRolledBack() throws Exception {
        registerDmlOperation(TRANSACTION_ID, SCN, ROW_ID);
        registerDmlOperation(OTHER_TRANSACTION_ID, OTHER_SCN, OTHER_ROW_ID);
        transactionalBuffer.rollback(TRANSACTION_ID, "");
        assertThat(transactionalBuffer.isEmpty()).isFalse();
        assertThat(transactionalBuffer.getRolledBackTransactionIds().contains(TRANSACTION_ID)).isTrue();
        assertThat(transactionalBuffer.getRolledBackTransactionIds().contains(OTHER_TRANSACTION_ID)).isFalse();
    }

    @Test
    public void testNonEmptySecondTransactionIsRolledBack() throws Exception {
        registerDmlOperation(TRANSACTION_ID, SCN, ROW_ID);
        registerDmlOperation(OTHER_TRANSACTION_ID, OTHER_SCN, OTHER_ROW_ID);
        transactionalBuffer.rollback(OTHER_TRANSACTION_ID, "");
        assertThat(transactionalBuffer.isEmpty()).isFalse();
        assertThat(transactionalBuffer.getRolledBackTransactionIds().contains(TRANSACTION_ID)).isFalse();
        assertThat(transactionalBuffer.getRolledBackTransactionIds().contains(OTHER_TRANSACTION_ID)).isTrue();
    }

    @Test
    public void testCalculateScnWhenTransactionIsCommitted() throws Exception {
        registerDmlOperation(TRANSACTION_ID, SCN, ROW_ID);
        offsetContext = new OracleOffsetContext(connectorConfig, SCN, SCN, null, false, true, new TransactionContext());
        transactionalBuffer.commit(TRANSACTION_ID, SCN.add(SCN_ONE), offsetContext, TIMESTAMP, () -> true, MESSAGE, dispatcher);
        assertThat(streamingMetrics.getOldestScn()).isEqualTo(SCN.toString());
        assertThat(transactionalBuffer.getRolledBackTransactionIds().isEmpty()).isTrue();
    }

    @Test
    public void testCalculateScnWhenFirstTransactionIsCommitted() throws Exception {
        registerDmlOperation(TRANSACTION_ID, SCN, ROW_ID);
        registerDmlOperation(OTHER_TRANSACTION_ID, OTHER_SCN, OTHER_ROW_ID);
        offsetContext = new OracleOffsetContext(connectorConfig, SCN, SCN, null, false, true, new TransactionContext());
        transactionalBuffer.commit(TRANSACTION_ID, SCN.add(SCN_ONE), offsetContext, TIMESTAMP, () -> true, MESSAGE, dispatcher);

        // after commit, it stays the same because OTHER_TRANSACTION_ID is not committed yet
        assertThat(streamingMetrics.getOldestScn()).isEqualTo(SCN.toString());
        assertThat(transactionalBuffer.getRolledBackTransactionIds().isEmpty()).isTrue();

        transactionalBuffer.commit(OTHER_TRANSACTION_ID, OTHER_SCN.add(SCN_ONE), offsetContext, TIMESTAMP, () -> true, MESSAGE, dispatcher);
        assertThat(streamingMetrics.getOldestScn()).isEqualTo(OTHER_SCN.toString());
    }

    @Test
    public void testCalculateScnWhenSecondTransactionIsCommitted() throws Exception {
        registerDmlOperation(TRANSACTION_ID, SCN, ROW_ID);
        registerDmlOperation(OTHER_TRANSACTION_ID, OTHER_SCN, OTHER_ROW_ID);
        offsetContext = new OracleOffsetContext(connectorConfig, OTHER_SCN, OTHER_SCN, null, false, true, new TransactionContext());
        transactionalBuffer.commit(OTHER_TRANSACTION_ID, OTHER_SCN.add(SCN_ONE), offsetContext, TIMESTAMP, () -> true, MESSAGE, dispatcher);

        assertThat(streamingMetrics.getOldestScn()).isEqualTo(SCN.toString());
        // after committing OTHER_TRANSACTION_ID
        assertThat(transactionalBuffer.getRolledBackTransactionIds().isEmpty()).isTrue();
    }

    @Test
    public void testAbandoningOneTransaction() {
        registerDmlOperation(TRANSACTION_ID, SCN, ROW_ID);
        offsetContext = new OracleOffsetContext(connectorConfig, SCN, SCN, (LcrPosition) null, false, true, new TransactionContext());
        transactionalBuffer.abandonLongTransactions(SCN, offsetContext);
        assertThat(transactionalBuffer.isEmpty()).isTrue();
    }

    @Test
    public void testAbandoningTransactionHavingAnotherOne() {
        registerDmlOperation(TRANSACTION_ID, SCN, ROW_ID);
        registerDmlOperation(OTHER_TRANSACTION_ID, OTHER_SCN, OTHER_ROW_ID);
        transactionalBuffer.abandonLongTransactions(SCN, offsetContext);
        assertThat(transactionalBuffer.isEmpty()).isFalse();
    }

    @Test
    public void testTransactionDump() {
        registerDmlOperation(TRANSACTION_ID, SCN, ROW_ID);
        registerDmlOperation(OTHER_TRANSACTION_ID, OTHER_SCN, OTHER_ROW_ID);
        registerDmlOperation(OTHER_TRANSACTION_ID, OTHER_SCN, OTHER_ROW_ID);
        assertThat(transactionalBuffer.toString()).contains(String.valueOf(SCN));
        assertThat(transactionalBuffer.toString()).contains(String.valueOf(OTHER_SCN));
    }

    private void registerDmlOperation(String txId, Scn scn, String rowId) {
        transactionalBuffer.registerDmlOperation(RowMapper.INSERT, txId, scn, TABLE_ID, DML_ENTRY, Instant.now(), rowId);
    }
}
