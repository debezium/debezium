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
        assertThat(lag == 0).isTrue();
        Instant dbEventTime = Instant.now().minusMillis(2000);
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSource();
        assertThat(lag == 2000).isTrue();
        assertThat(metrics.getMaxLagFromSource() == 2000).isTrue();
        assertThat(metrics.getMinLagFromSource() == 0).isTrue();
        assertThat(metrics.getAverageLagFromSource() == 2000).isTrue();

        // not realistic scenario
        dbEventTime = Instant.now().plusMillis(2000);
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSource();
        assertThat(lag == -2000).isTrue();
        assertThat(metrics.getMaxLagFromSource() == 2000).isTrue();
        assertThat(metrics.getMinLagFromSource() == -2000).isTrue();
        assertThat(metrics.getAverageLagFromSource() == 0).isTrue();

        metrics.reset();

        // ##########################
        // the database time is ahead
        metrics.setTimeDifference(new AtomicLong(-1000));
        dbEventTime = Instant.now().minusMillis(2000);
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSource();
        assertThat(lag == 3000).isTrue();
        assertThat(metrics.getMaxLagFromSource() == 3000).isTrue();
        assertThat(metrics.getMinLagFromSource() == 0).isTrue();
        assertThat(metrics.getAverageLagFromSource() == 3000).isTrue();

        dbEventTime = Instant.now().minusMillis(3000);
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSource();
        assertThat(lag == 4000).isTrue();
        assertThat(metrics.getMaxLagFromSource() == 4000).isTrue();
        assertThat(metrics.getMinLagFromSource() == 0).isTrue();
        assertThat(metrics.getAverageLagFromSource() == 3500).isTrue();

        metrics.reset();

        // ##########################
        // the database time is behind
        metrics.setTimeDifference(new AtomicLong(1000));
        dbEventTime = Instant.now().minusMillis(2000);
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSource();
        assertThat(lag == 1000).isTrue();
        assertThat(metrics.getMaxLagFromSource() == 1000).isTrue();
        assertThat(metrics.getMinLagFromSource() == 0).isTrue();
        assertThat(metrics.getAverageLagFromSource() == 1000).isTrue();
    }

    @Test
    public void testOtherMetrics() {
        metrics.incrementScnFreezeCounter();
        assertThat(metrics.getScnFreezeCounter() == 1).isTrue();

        metrics.incrementErrorCounter();
        assertThat(metrics.getErrorCounter() == 1).isTrue();

        metrics.incrementWarningCounter();
        assertThat(metrics.getWarningCounter() == 1).isTrue();

        metrics.incrementCommittedDmlCounter(5_000);
        for (int i = 0; i < 1000; i++) {
            metrics.incrementCapturedDmlCounter();
            metrics.incrementCommittedTransactions();
        }
        assertThat(metrics.getCapturedDmlCount() == 1000).isTrue();
        assertThat(metrics.getCapturedDmlThroughput() > 10_000).isTrue();
        assertThat(metrics.getNumberOfCommittedTransactions() == 1000).isTrue();
        assertThat(metrics.getCommitThroughput() >= 1_000).isTrue();

        metrics.incrementRolledBackTransactions();
        assertThat(metrics.getNumberOfRolledBackTransactions() == 1).isTrue();

        metrics.setActiveTransactions(5);
        assertThat(metrics.getNumberOfActiveTransactions() == 5).isTrue();

        metrics.addRolledBackTransactionId("rolledback id");
        assertThat(metrics.getNumberOfRolledBackTransactions() == 1).isTrue();
        assertThat(metrics.getRolledBackTransactionIds().contains("rolledback id")).isTrue();

        metrics.addAbandonedTransactionId("abandoned id");
        assertThat(metrics.getAbandonedTransactionIds().size() == 1).isTrue();
        assertThat(metrics.getAbandonedTransactionIds().contains("abandoned id")).isTrue();

        metrics.setOldestScn(10L);
        assertThat(metrics.getOldestScn() == 10L).isTrue();

        metrics.setCommittedScn(10L);
        assertThat(metrics.getCommittedScn() == 10L).isTrue();

        assertThat(metrics.toString().contains("capturedDmlCounter=1000")).isTrue();

    }
}
