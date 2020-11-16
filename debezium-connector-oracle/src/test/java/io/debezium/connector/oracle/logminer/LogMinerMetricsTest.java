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

        metrics.setLastCapturedDmlCount(1);
        assertThat(metrics.getTotalCapturedDmlCount() == 1).isTrue();

        metrics.setCurrentScn(1000L);
        assertThat(metrics.getCurrentScn()).isEqualTo(1000L);

        metrics.setBatchSize(10);
        assertThat(metrics.getBatchSize() == LogMinerMetrics.DEFAULT_BATCH_SIZE).isTrue();
        metrics.setBatchSize(1_000_000);
        assertThat(metrics.getBatchSize()).isEqualTo(LogMinerMetrics.DEFAULT_BATCH_SIZE);
        metrics.setBatchSize(6000);
        assertThat(metrics.getBatchSize()).isEqualTo(6_000);

        assertThat(metrics.getMillisecondToSleepBetweenMiningQuery()).isEqualTo(1000);
        metrics.changeSleepingTime(true);
        assertThat(metrics.getMillisecondToSleepBetweenMiningQuery()).isEqualTo(1200);
        metrics.changeSleepingTime(false);
        assertThat(metrics.getMillisecondToSleepBetweenMiningQuery()).isEqualTo(1000);
        metrics.setMillisecondToSleepBetweenMiningQuery(-1);
        assertThat(metrics.getMillisecondToSleepBetweenMiningQuery()).isEqualTo(1000);
        metrics.setMillisecondToSleepBetweenMiningQuery(4000);
        assertThat(metrics.getMillisecondToSleepBetweenMiningQuery()).isEqualTo(1000);
        metrics.setMillisecondToSleepBetweenMiningQuery(2000);
        assertThat(metrics.getMillisecondToSleepBetweenMiningQuery()).isEqualTo(2000);

        metrics.setLastDurationOfBatchCapturing(Duration.ofMillis(100));
        assertThat(metrics.getLastDurationOfFetchingQuery()).isEqualTo(100);
        metrics.setLastDurationOfBatchCapturing(Duration.ofMillis(200));
        assertThat(metrics.getLastDurationOfFetchingQuery()).isEqualTo(200);
        assertThat(metrics.getMaxDurationOfFetchingQuery()).isEqualTo(200);
        assertThat(metrics.getFetchingQueryCount()).isEqualTo(2);

        metrics.setCurrentLogFileName(new HashSet<>(Arrays.asList("name", "name1")));
        assertThat(metrics.getCurrentRedoLogFileName()[0].equals("name")).isTrue();
        assertThat(metrics.getCurrentRedoLogFileName()[1].equals("name1")).isTrue();

        metrics.setSwitchCount(5);
        assertThat(metrics.getSwitchCounter() == 5).isTrue();

        metrics.reset();
        metrics.setLastDurationOfBatchCapturing(Duration.ofMillis(1000));
        assertThat(metrics.getLastDurationOfFetchingQuery()).isEqualTo(1000);
        assertThat(metrics.getFetchingQueryCount()).isEqualTo(1);

        metrics.reset();
        metrics.setLastCapturedDmlCount(300);
        metrics.setLastDurationOfBatchProcessing(Duration.ofMillis(1000));
        assertThat(metrics.getLastCapturedDmlCount()).isEqualTo(300);
        assertThat(metrics.getLastBatchProcessingDuration()).isEqualTo(1000);
        assertThat(metrics.getAverageBatchProcessingThroughput()).isGreaterThanOrEqualTo(300);
        assertThat(metrics.getMaxCapturedDmlInBatch()).isEqualTo(300);
        assertThat(metrics.getMaxBatchProcessingThroughput()).isEqualTo(300);

        metrics.setLastCapturedDmlCount(500);
        metrics.setLastDurationOfBatchProcessing(Duration.ofMillis(1000));
        assertThat(metrics.getAverageBatchProcessingThroughput()).isEqualTo(400);
        assertThat(metrics.getMaxCapturedDmlInBatch()).isEqualTo(500);
        assertThat(metrics.getMaxBatchProcessingThroughput()).isEqualTo(500);
        assertThat(metrics.getLastBatchProcessingThroughput()).isEqualTo(500);

        metrics.setLastDurationOfBatchProcessing(Duration.ofMillis(5000));
        assertThat(metrics.getLastBatchProcessingThroughput()).isEqualTo(100);

        metrics.setLastDurationOfBatchProcessing(Duration.ZERO);
        assertThat(metrics.getLastBatchProcessingThroughput()).isEqualTo(0);

        assertThat(metrics.getHoursToKeepTransactionInBuffer()).isEqualTo(4);

        metrics.setRedoLogStatus(Collections.singletonMap("name", "current"));
        assertThat(metrics.getRedoLogStatus()[0].equals("name | current")).isTrue();

        assertThat(metrics.toString().contains("logMinerQueryCount"));

        assertThat(metrics.getRecordMiningHistory()).isFalse();
        metrics.setRecordMiningHistory(true);
        assertThat(metrics.getRecordMiningHistory()).isTrue();

        metrics.setHoursToKeepTransactionInBuffer(3);
        assertThat(metrics.getHoursToKeepTransactionInBuffer()).isEqualTo(3);

        metrics.incrementNetworkConnectionProblemsCounter();
        assertThat(metrics.getNetworkConnectionProblemsCounter()).isEqualTo(1);

        metrics.setBatchSize(5000);
        metrics.changeBatchSize(true);
        assertThat(metrics.getBatchSize()).isEqualTo(6000);
        metrics.changeBatchSize(false);
        assertThat(metrics.getBatchSize()).isEqualTo(5000);
    }

}
