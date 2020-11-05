/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIs;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName;

@SkipWhenAdapterNameIsNot(value = AdapterName.LOGMINER)
public class TransactionalBufferMetricsTest {

    private TransactionalBufferMetrics metrics;

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    @Before
    public void before() {
        CdcSourceTaskContext taskContext = mock(CdcSourceTaskContext.class);
        Mockito.when(taskContext.getConnectorName()).thenReturn("connector name");
        Mockito.when(taskContext.getConnectorType()).thenReturn("connector type");
        metrics = new TransactionalBufferMetrics(taskContext);
    }

    @After
    public void after() {
        metrics.reset();
    }

    @Test
    @SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.LOGMINER, reason = "Lag calculations fail")
    public void testLagMetrics() {
        // no time difference between connector and database
        long lag = metrics.getLagFromSource();
        assertThat(lag).isEqualTo(0);
        Instant dbEventTime = Instant.now().minusMillis(2000);
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSource();
        assertThat(lag).isEqualTo(2000);
        assertThat(metrics.getMaxLagFromSource()).isEqualTo(2000);
        assertThat(metrics.getMinLagFromSource()).isEqualTo(0);

        // not realistic scenario
        dbEventTime = Instant.now().plusMillis(2000);
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSource();
        assertThat(lag).isEqualTo(2000);
        assertThat(metrics.getMaxLagFromSource()).isEqualTo(2000);
        assertThat(metrics.getMinLagFromSource()).isEqualTo(0);

        metrics.reset();

        // ##########################
        // the database time is ahead
        metrics.setTimeDifference(new AtomicLong(-1000));
        dbEventTime = Instant.now().minusMillis(2000);
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSource();
        assertThat(lag).isEqualTo(3000);
        assertThat(metrics.getMaxLagFromSource()).isEqualTo(3000);
        assertThat(metrics.getMinLagFromSource()).isEqualTo(0);

        dbEventTime = Instant.now().minusMillis(3000);
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSource();
        assertThat(lag).isEqualTo(4000);
        assertThat(metrics.getMaxLagFromSource()).isEqualTo(4000);
        assertThat(metrics.getMinLagFromSource()).isEqualTo(0);

        metrics.reset();

        // ##########################
        // the database time is behind
        metrics.setTimeDifference(new AtomicLong(1000));
        dbEventTime = Instant.now().minusMillis(2000);
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSource();
        assertThat(lag).isEqualTo(1000);
        assertThat(metrics.getMaxLagFromSource()).isEqualTo(1000);
        assertThat(metrics.getMinLagFromSource()).isEqualTo(0);
    }

    @Test
    public void testOtherMetrics() {
        metrics.incrementScnFreezeCounter();
        assertThat(metrics.getScnFreezeCounter()).isEqualTo(1);

        metrics.incrementErrorCounter();
        assertThat(metrics.getErrorCounter()).isEqualTo(1);

        metrics.incrementWarningCounter();
        assertThat(metrics.getWarningCounter()).isEqualTo(1);

        metrics.incrementCommittedDmlCounter(5_000);
        for (int i = 0; i < 1000; i++) {
            metrics.incrementRegisteredDmlCounter();
            metrics.incrementCommittedTransactions();
        }
        assertThat(metrics.getRegisteredDmlCount()).isEqualTo(1000);
        assertThat(metrics.getNumberOfCommittedTransactions()).isEqualTo(1000);
        assertThat(metrics.getCommitThroughput()).isGreaterThanOrEqualTo(1_000);

        metrics.incrementRolledBackTransactions();
        assertThat(metrics.getNumberOfRolledBackTransactions()).isEqualTo(1);

        metrics.setActiveTransactions(5);
        assertThat(metrics.getNumberOfActiveTransactions()).isEqualTo(5);

        metrics.addRolledBackTransactionId("rolledback id");
        assertThat(metrics.getNumberOfRolledBackTransactions()).isEqualTo(1);
        assertThat(metrics.getRolledBackTransactionIds().contains("rolledback id")).isTrue();

        metrics.addAbandonedTransactionId("abandoned id");
        assertThat(metrics.getAbandonedTransactionIds().size()).isEqualTo(1);
        assertThat(metrics.getAbandonedTransactionIds().contains("abandoned id")).isTrue();

        metrics.setOldestScn(10L);
        assertThat(metrics.getOldestScn()).isEqualTo(10);

        metrics.setCommittedScn(10L);
        assertThat(metrics.getCommittedScn()).isEqualTo(10);

        assertThat(metrics.toString().contains("registeredDmlCounter=1000")).isTrue();

        metrics.setLastCommitDuration(100L);
        assertThat(metrics.getLastCommitDuration()).isEqualTo(100L);

        metrics.setLastCommitDuration(50L);
        assertThat(metrics.getMaxCommitDuration()).isEqualTo(100L);

        metrics.setOffsetScn(10L);
        assertThat(metrics.getOldestScn() == 10).isTrue();

        metrics.setCommitQueueCapacity(1000);
        assertThat(metrics.getCommitQueueCapacity()).isEqualTo(1000);

    }
}
