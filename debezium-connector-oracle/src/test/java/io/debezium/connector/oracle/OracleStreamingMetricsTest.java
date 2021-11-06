/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE;
import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE;
import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.DataChangeEvent;

/**
 * Test streaming metrics.
 */
public class OracleStreamingMetricsTest {

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    private OracleConnectorConfig connectorConfig;
    private OracleStreamingChangeEventSourceMetrics metrics;
    private Clock fixedClock;

    @Before
    public void before() {
        init(TestHelper.defaultConfig());
    }

    @Test
    public void testMetrics() {
        metrics.setLastCapturedDmlCount(1);
        assertThat(metrics.getTotalCapturedDmlCount() == 1).isTrue();

        metrics.setCurrentScn(Scn.valueOf(1000L));
        assertThat(metrics.getCurrentScn()).isEqualTo("1000");

        metrics.setBatchSize(10);
        assertThat(metrics.getBatchSize() == connectorConfig.getLogMiningBatchSizeDefault()).isTrue();
        metrics.setBatchSize(1_000_000);
        assertThat(metrics.getBatchSize()).isEqualTo(connectorConfig.getLogMiningBatchSizeDefault());
        metrics.setBatchSize(6000);
        assertThat(metrics.getBatchSize()).isEqualTo(6_000);

        assertThat(metrics.getMillisecondToSleepBetweenMiningQuery()).isEqualTo(1000);
        metrics.changeSleepingTime(true);
        assertThat(metrics.getMillisecondToSleepBetweenMiningQuery()).isEqualTo(1200);
        metrics.changeSleepingTime(false);
        assertThat(metrics.getMillisecondToSleepBetweenMiningQuery()).isEqualTo(1000);
        metrics.setMillisecondToSleepBetweenMiningQuery(-1L);
        assertThat(metrics.getMillisecondToSleepBetweenMiningQuery()).isEqualTo(1000);
        metrics.setMillisecondToSleepBetweenMiningQuery(4000L);
        assertThat(metrics.getMillisecondToSleepBetweenMiningQuery()).isEqualTo(1000);
        metrics.setMillisecondToSleepBetweenMiningQuery(2000L);
        assertThat(metrics.getMillisecondToSleepBetweenMiningQuery()).isEqualTo(2000);

        metrics.setLastDurationOfBatchCapturing(Duration.ofMillis(100));
        assertThat(metrics.getLastDurationOfFetchQueryInMilliseconds()).isEqualTo(100);
        metrics.setLastDurationOfBatchCapturing(Duration.ofMillis(200));
        assertThat(metrics.getLastDurationOfFetchQueryInMilliseconds()).isEqualTo(200);
        assertThat(metrics.getMaxDurationOfFetchQueryInMilliseconds()).isEqualTo(200);
        assertThat(metrics.getFetchingQueryCount()).isEqualTo(2);

        metrics.setCurrentLogFileName(new HashSet<>(Arrays.asList("name", "name1")));
        assertThat(metrics.getCurrentRedoLogFileName()[0].equals("name")).isTrue();
        assertThat(metrics.getCurrentRedoLogFileName()[1].equals("name1")).isTrue();

        metrics.setSwitchCount(5);
        assertThat(metrics.getSwitchCounter() == 5).isTrue();

        metrics.reset();
        metrics.setLastDurationOfBatchCapturing(Duration.ofMillis(1000));
        assertThat(metrics.getLastDurationOfFetchQueryInMilliseconds()).isEqualTo(1000);
        assertThat(metrics.getFetchingQueryCount()).isEqualTo(1);

        metrics.reset();
        metrics.setLastCapturedDmlCount(300);
        metrics.setLastDurationOfBatchProcessing(Duration.ofMillis(1000));
        assertThat(metrics.getLastCapturedDmlCount()).isEqualTo(300);
        assertThat(metrics.getLastBatchProcessingTimeInMilliseconds()).isEqualTo(1000);
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

        assertThat(metrics.getHoursToKeepTransactionInBuffer()).isEqualTo(0);

        metrics.setRedoLogStatus(Collections.singletonMap("name", "current"));
        assertThat(metrics.getRedoLogStatus()[0].equals("name | current")).isTrue();

        assertThat(metrics.toString().contains("logMinerQueryCount"));

        metrics.incrementNetworkConnectionProblemsCounter();
        assertThat(metrics.getNetworkConnectionProblemsCounter()).isEqualTo(1);

        metrics.setBatchSize(5000);
        metrics.changeBatchSize(true, false);
        assertThat(metrics.getBatchSize()).isEqualTo(6000);
        metrics.changeBatchSize(false, false);
        assertThat(metrics.getBatchSize()).isEqualTo(5000);
    }

    @Test
    public void testLagMetrics() {
        // no time difference between connector and database
        long lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(0);
        Instant dbEventTime = fixedClock.instant().minusMillis(2000);
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(2000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(2000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(2000);

        // not realistic scenario
        dbEventTime = fixedClock.instant().plusMillis(3000);
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(3000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(3000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(2000);

        metrics.reset();

        // ##########################
        // the database time is ahead 1s and has an offset of +12h
        OffsetDateTime dbTime = OffsetDateTime.parse("2021-05-16T00:30:01.00+12:00");
        metrics.calculateTimeDifference(dbTime);

        dbEventTime = Instant.parse("2021-05-16T00:29:58.00Z");
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(3000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(3000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(3000);

        dbEventTime = Instant.parse("2021-05-16T00:29:57.00Z");
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(4000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(4000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(3000);

        metrics.reset();

        // ##########################
        // the database time is ahead 1s and has an offset of +0h (UTC)
        dbTime = OffsetDateTime.parse("2021-05-15T12:30:01.00Z");
        metrics.calculateTimeDifference(dbTime);

        dbEventTime = Instant.parse("2021-05-15T12:29:58.00Z");
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(3000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(3000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(3000);

        dbEventTime = Instant.parse("2021-05-15T12:29:57.00Z");
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(4000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(4000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(3000);

        metrics.reset();

        // ##########################
        // the database time is ahead 1s and has an offset of -12h
        dbTime = OffsetDateTime.parse("2021-05-15T00:30:01.00-12:00");
        metrics.calculateTimeDifference(dbTime);

        dbEventTime = Instant.parse("2021-05-15T00:29:58.00Z");
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(3000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(3000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(3000);

        dbEventTime = Instant.parse("2021-05-15T00:29:57.00Z");
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(4000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(4000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(3000);

        metrics.reset();

        // ##########################
        // the database time is behind 1s and has an offset of +12h
        dbTime = OffsetDateTime.parse("2021-05-16T00:29:59.00+12:00");
        metrics.calculateTimeDifference(dbTime);

        dbEventTime = Instant.parse("2021-05-16T00:29:58.00Z");
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(1000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(1000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(1000);

        // ##########################
        // the database time is behind 1s and has an offset of +0h (UTC)
        dbTime = OffsetDateTime.parse("2021-05-15T12:29:59.00Z");
        metrics.calculateTimeDifference(dbTime);

        dbEventTime = Instant.parse("2021-05-15T12:29:58.00Z");
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(1000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(1000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(1000);

        // ##########################
        // the database time is behind 1s and has an offset of -12h
        dbTime = OffsetDateTime.parse("2021-05-15T00:29:59.00-12:00");
        metrics.calculateTimeDifference(dbTime);

        dbEventTime = Instant.parse("2021-05-15T00:29:58.00Z");
        metrics.calculateLagMetrics(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(1000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(1000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(1000);
    }

    @Test
    public void testOtherMetrics() {
        metrics.incrementScnFreezeCount();
        assertThat(metrics.getScnFreezeCount()).isEqualTo(1);

        metrics.incrementErrorCount();
        assertThat(metrics.getErrorCount()).isEqualTo(1);

        metrics.incrementWarningCount();
        assertThat(metrics.getWarningCount()).isEqualTo(1);

        metrics.incrementCommittedDmlCount(5_000);
        for (int i = 0; i < 1000; i++) {
            metrics.incrementRegisteredDmlCount();
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

        metrics.setOldestScn(Scn.valueOf(10L));
        assertThat(metrics.getOldestScn()).isEqualTo("10");

        metrics.setCommittedScn(Scn.valueOf(10L));
        assertThat(metrics.getCommittedScn()).isEqualTo("10");

        assertThat(metrics.toString().contains("registeredDmlCount=1000")).isTrue();

        metrics.setLastCommitDuration(Duration.ofMillis(100L));
        assertThat(metrics.getLastCommitDurationInMilliseconds()).isEqualTo(100L);

        metrics.setLastCommitDuration(Duration.ofMillis(50L));
        assertThat(metrics.getMaxCommitDurationInMilliseconds()).isEqualTo(100L);

        metrics.setOffsetScn(Scn.valueOf(10L));
        assertThat(metrics.getOldestScn()).isEqualTo("10");
    }

    @Test
    @FixFor("DBZ-2754")
    public void testCustomTransactionRetention() throws Exception {
        init(TestHelper.defaultConfig().with(OracleConnectorConfig.LOG_MINING_TRANSACTION_RETENTION, 3));
        assertThat(metrics.getHoursToKeepTransactionInBuffer()).isEqualTo(3);
    }

    private void init(Configuration.Builder builder) {
        this.connectorConfig = new OracleConnectorConfig(builder.build());

        final ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(Duration.of(DEFAULT_MAX_QUEUE_SIZE, ChronoUnit.MILLIS))
                .maxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                .maxQueueSize(DEFAULT_MAX_QUEUE_SIZE)
                .build();

        final OracleTaskContext taskContext = mock(OracleTaskContext.class);
        Mockito.when(taskContext.getConnectorName()).thenReturn("connector name");
        Mockito.when(taskContext.getConnectorType()).thenReturn("connector type");

        final OracleEventMetadataProvider metadataProvider = new OracleEventMetadataProvider();
        fixedClock = Clock.fixed(Instant.parse("2021-05-15T12:30:00.00Z"), ZoneOffset.UTC);
        this.metrics = new OracleStreamingChangeEventSourceMetrics(taskContext, queue, metadataProvider, connectorConfig, fixedClock);
    }
}
