/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName;

@SkipWhenAdapterNameIsNot(value = AdapterName.LOGMINER)
public class LogMinerMetricsTest {

    private LogMinerMetrics metrics;

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    @Before
    public void before() {
        CdcSourceTaskContext taskContext = mock(CdcSourceTaskContext.class);
        Mockito.when(taskContext.getConnectorName()).thenReturn("connector name");
        Mockito.when(taskContext.getConnectorType()).thenReturn("connector type");
        metrics = new LogMinerMetrics(taskContext);
    }

    @Test
    public void testMetrics() {

        metrics.incrementCapturedDmlCount();
        assertThat(metrics.getCapturedDmlCount() == 1).isTrue();

        metrics.setCurrentScn(1000L);
        assertThat(metrics.getCurrentScn() == 1000L).isTrue();

        metrics.setBatchSize(10);
        assertThat(metrics.getBatchSize() == 5_000).isTrue();
        metrics.setBatchSize(1_000_000);
        assertThat(metrics.getBatchSize() == 5_000).isTrue();
        metrics.setBatchSize(6000);
        assertThat(metrics.getBatchSize() == 6_000).isTrue();

        assertThat(metrics.getMillisecondToSleepBetweenMiningQuery() == 1000).isTrue();
        metrics.changeSleepingTime(true);
        assertThat(metrics.getMillisecondToSleepBetweenMiningQuery() == 1200).isTrue();
        metrics.changeSleepingTime(false);
        assertThat(metrics.getMillisecondToSleepBetweenMiningQuery() == 1000).isTrue();
        metrics.setMillisecondToSleepBetweenMiningQuery(20);
        assertThat(metrics.getMillisecondToSleepBetweenMiningQuery() == 1000).isTrue();
        metrics.setMillisecondToSleepBetweenMiningQuery(4000);
        assertThat(metrics.getMillisecondToSleepBetweenMiningQuery() == 1000).isTrue();
        metrics.setMillisecondToSleepBetweenMiningQuery(2000);
        assertThat(metrics.getMillisecondToSleepBetweenMiningQuery() == 2000).isTrue();

        metrics.setLastLogMinerQueryDuration(Duration.ofMillis(100));
        assertThat(metrics.getLastLogMinerQueryDuration() == 100).isTrue();
        metrics.setLastLogMinerQueryDuration(Duration.ofMillis(200));
        assertThat(metrics.getLastLogMinerQueryDuration() == 200).isTrue();
        assertThat(metrics.getAverageLogMinerQueryDuration() == 150).isTrue();
        assertThat(metrics.getLogMinerQueryCount() == 2).isTrue();

        metrics.setCurrentLogFileName(new HashSet<>(Arrays.asList("name", "name1")));
        assertThat(metrics.getCurrentRedoLogFileName()[0].equals("name")).isTrue();
        assertThat(metrics.getCurrentRedoLogFileName()[1].equals("name1")).isTrue();

        metrics.setSwitchCount(5);
        assertThat(metrics.getSwitchCounter() == 5).isTrue();

        metrics.setProcessedCapturedBatchDuration(Duration.ofMillis(1000));
        assertThat(metrics.getLastProcessedCapturedBatchDuration() == 1000).isTrue();
        assertThat(metrics.getProcessedCapturedBatchCount() == 1).isTrue();
        assertThat(metrics.getAverageProcessedCapturedBatchDuration() == 1000).isTrue();

        metrics.setRedoLogStatus(Collections.singletonMap("name", "current"));
        assertThat(metrics.getRedoLogStatus()[0].equals("name | current")).isTrue();

        assertThat(metrics.toString().contains("logMinerQueryCount"));
    }

}
