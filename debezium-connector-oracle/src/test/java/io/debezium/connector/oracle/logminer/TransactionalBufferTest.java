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

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

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
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName;
import io.debezium.connector.oracle.xstream.LcrPosition;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.txmetadata.TransactionContext;

/**
 * @author Andrey Pustovetov
 */
@SkipWhenAdapterNameIsNot(value = AdapterName.LOGMINER)
public class TransactionalBufferTest {

    private static final String SERVER_NAME = "serverX";
    private static final String TRANSACTION_ID = "transaction";
    private static final String OTHER_TRANSACTION_ID = "other_transaction";
    private static final String SQL_ONE = "update table";
    private static final String SQL_TWO = "insert into table";
    private static final String MESSAGE = "OK";
    private static final Scn SCN = Scn.ONE;
    private static final Scn OTHER_SCN = Scn.fromLong(10L);
    private static final Scn LARGEST_SCN = Scn.fromLong(100L);
    private static final Timestamp TIMESTAMP = new Timestamp(System.currentTimeMillis());
    private static final Configuration config = new Configuration() {
        @Override
        public Set<String> keys() {
            return null;
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
    private TransactionalBufferMetrics metrics;
    private EventDispatcher<?> dispatcher;

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

        dispatcher = mock(EventDispatcher.class);

        transactionalBuffer = new TransactionalBuffer(taskContext, errorHandler);
        metrics = transactionalBuffer.getMetrics();
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
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
        });
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(false);
    }

    @Test
    public void testIsEmptyWhenTransactionIsCommitted() throws InterruptedException {
        CountDownLatch commitLatch = new CountDownLatch(1);
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> commitLatch.countDown());
        offsetContext = new OracleOffsetContext(connectorConfig, SCN.longValue(), SCN.longValue(), (LcrPosition) null, false, true, new TransactionContext());
        transactionalBuffer.commit(TRANSACTION_ID, SCN.add(Scn.ONE), offsetContext, TIMESTAMP, () -> true, MESSAGE, dispatcher);
        commitLatch.await();
        Thread.sleep(1000);
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(true);
    }

    @Test
    public void testIsEmptyWhenTransactionIsRolledBack() {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
        });
        transactionalBuffer.rollback(TRANSACTION_ID, "");
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(true);
        assertThat(transactionalBuffer.getLargestScn()).isEqualTo(SCN);
    }

    @Test
    public void testNonEmptyFirstTransactionIsRolledBack() {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
        });
        transactionalBuffer.registerCommitCallback(OTHER_TRANSACTION_ID, OTHER_SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
        });
        transactionalBuffer.rollback(TRANSACTION_ID, "");
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(false);
        assertThat(transactionalBuffer.getLargestScn()).isEqualTo(OTHER_SCN);
        assertThat(transactionalBuffer.getRolledBackTransactionIds().contains(TRANSACTION_ID)).isTrue();
        assertThat(transactionalBuffer.getRolledBackTransactionIds().contains(OTHER_TRANSACTION_ID)).isFalse();
    }

    @Test
    public void testNonEmptySecondTransactionIsRolledBack() {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
        });
        transactionalBuffer.registerCommitCallback(OTHER_TRANSACTION_ID, OTHER_SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
        });
        transactionalBuffer.rollback(OTHER_TRANSACTION_ID, "");
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(false);
        assertThat(transactionalBuffer.getLargestScn()).isEqualTo(OTHER_SCN);
        assertThat(transactionalBuffer.getRolledBackTransactionIds().contains(TRANSACTION_ID)).isFalse();
        assertThat(transactionalBuffer.getRolledBackTransactionIds().contains(OTHER_TRANSACTION_ID)).isTrue();
    }

    @Test
    public void testCalculateScnWhenTransactionIsCommitted() throws InterruptedException {
        CountDownLatch commitLatch = new CountDownLatch(1);
        AtomicReference<Scn> smallestScnContainer = new AtomicReference<>();
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
            smallestScnContainer.set(smallestScn);
            commitLatch.countDown();
        });
        assertThat(transactionalBuffer.getLargestScn()).isEqualTo(SCN); // before commit
        offsetContext = new OracleOffsetContext(connectorConfig, SCN.longValue(), SCN.longValue(), null, false, true, new TransactionContext());
        transactionalBuffer.commit(TRANSACTION_ID, SCN.add(Scn.ONE), offsetContext, TIMESTAMP, () -> true, MESSAGE, dispatcher);
        commitLatch.await();
        assertThat(transactionalBuffer.getLargestScn()).isEqualTo(SCN); // after commit

        assertThat(smallestScnContainer.get()).isNull();

        assertThat(transactionalBuffer.getRolledBackTransactionIds().isEmpty()).isTrue();
    }

    @Test
    public void testCalculateScnWhenFirstTransactionIsCommitted() throws InterruptedException {
        CountDownLatch commitLatch = new CountDownLatch(1);
        AtomicReference<Scn> smallestScnContainer = new AtomicReference<>();
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
            smallestScnContainer.set(smallestScn);
            commitLatch.countDown();
        });
        transactionalBuffer.registerCommitCallback(OTHER_TRANSACTION_ID, OTHER_SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
        });
        assertThat(transactionalBuffer.getLargestScn()).isEqualTo(OTHER_SCN); // before commit
        offsetContext = new OracleOffsetContext(connectorConfig, SCN.longValue(), SCN.longValue(), null, false, true, new TransactionContext());
        transactionalBuffer.commit(TRANSACTION_ID, SCN.add(Scn.ONE), offsetContext, TIMESTAMP, () -> true, MESSAGE, dispatcher);
        commitLatch.await();
        // after commit, it stays the same because OTHER_TRANSACTION_ID is not committed yet
        assertThat(transactionalBuffer.getLargestScn()).isEqualTo(OTHER_SCN);

        assertThat(smallestScnContainer.get()).isEqualTo(OTHER_SCN);
        assertThat(transactionalBuffer.getRolledBackTransactionIds().isEmpty()).isTrue();
    }

    @Test
    public void testCalculateScnWhenSecondTransactionIsCommitted() throws InterruptedException {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
        });
        CountDownLatch commitLatch = new CountDownLatch(1);
        AtomicReference<Scn> smallestScnContainer = new AtomicReference<>();
        transactionalBuffer.registerCommitCallback(OTHER_TRANSACTION_ID, OTHER_SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
            smallestScnContainer.set(smallestScn);
            commitLatch.countDown();
        });
        assertThat(transactionalBuffer.getLargestScn()).isEqualTo(OTHER_SCN); // before commit
        offsetContext = new OracleOffsetContext(connectorConfig, OTHER_SCN.longValue(), OTHER_SCN.longValue(), null, false, true, new TransactionContext());
        transactionalBuffer.commit(OTHER_TRANSACTION_ID, OTHER_SCN.add(Scn.ONE), offsetContext, TIMESTAMP, () -> true, MESSAGE, dispatcher);
        commitLatch.await();
        assertThat(smallestScnContainer.get()).isEqualTo(SCN);
        // after committing OTHER_TRANSACTION_ID
        assertThat(transactionalBuffer.getLargestScn()).isEqualTo(OTHER_SCN);
        assertThat(transactionalBuffer.getRolledBackTransactionIds().isEmpty()).isTrue();
    }

    @Test
    public void testResetLargestScn() {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
        });
        transactionalBuffer.registerCommitCallback(OTHER_TRANSACTION_ID, OTHER_SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
        });
        assertThat(transactionalBuffer.getLargestScn()).isEqualTo(OTHER_SCN); // before commit
        offsetContext = new OracleOffsetContext(connectorConfig, OTHER_SCN.longValue(), OTHER_SCN.longValue(), null, false, true, new TransactionContext());
        transactionalBuffer.commit(OTHER_TRANSACTION_ID, OTHER_SCN, offsetContext, TIMESTAMP, () -> true, MESSAGE, dispatcher);
        assertThat(transactionalBuffer.getLargestScn()).isEqualTo(OTHER_SCN); // after commit

        transactionalBuffer.resetLargestScn(null);
        assertThat(transactionalBuffer.getLargestScn()).isEqualTo(Scn.ZERO);
        transactionalBuffer.resetLargestScn(OTHER_SCN.longValue());
        assertThat(transactionalBuffer.getLargestScn()).isEqualTo(OTHER_SCN);
    }

    @Test
    public void testAbandoningOneTransaction() {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
        });
        offsetContext = new OracleOffsetContext(connectorConfig, SCN.longValue(), SCN.longValue(), (LcrPosition) null, false, true, new TransactionContext());
        transactionalBuffer.abandonLongTransactions(SCN.longValue(), offsetContext);
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(true);
        assertThat(transactionalBuffer.getLargestScn()).isEqualTo(Scn.ZERO);
    }

    @Test
    public void testAbandoningTransactionHavingAnotherOne() {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
        });
        transactionalBuffer.registerCommitCallback(OTHER_TRANSACTION_ID, OTHER_SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
        });
        transactionalBuffer.abandonLongTransactions(SCN.longValue(), offsetContext);
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(false);
        assertThat(transactionalBuffer.getLargestScn()).isEqualTo(OTHER_SCN);
    }

    @Test
    public void testTransactionDump() {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
        });
        transactionalBuffer.registerCommitCallback(OTHER_TRANSACTION_ID, OTHER_SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
        });
        transactionalBuffer.registerCommitCallback(OTHER_TRANSACTION_ID, OTHER_SCN, Instant.now(), (timestamp, smallestScn, commitScn, counter) -> {
        });
        assertThat(transactionalBuffer.toString()).contains(String.valueOf(SCN));
        assertThat(transactionalBuffer.toString()).contains(String.valueOf(OTHER_SCN));
    }

    private void commitTransaction(TransactionalBuffer.CommitCallback commitCallback) {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), commitCallback);
        offsetContext = new OracleOffsetContext(connectorConfig, SCN.longValue(), SCN.longValue(), null, false, true, new TransactionContext());
        transactionalBuffer.commit(TRANSACTION_ID, SCN.add(Scn.ONE), offsetContext, TIMESTAMP, () -> true, MESSAGE, dispatcher);
    }
}
