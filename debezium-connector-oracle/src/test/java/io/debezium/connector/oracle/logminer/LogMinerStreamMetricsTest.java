/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Test;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleStreamingMetricsTest;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(SkipWhenAdapterNameIsNot.AdapterName.LOGMINER)
public class LogMinerStreamMetricsTest extends OracleStreamingMetricsTest<LogMinerStreamingChangeEventSourceMetrics> {

    @Override
    protected LogMinerStreamingChangeEventSourceMetrics createMetrics(OracleTaskContext taskContext,
                                                                      ChangeEventQueue<DataChangeEvent> queue,
                                                                      EventMetadataProvider metadataProvider,
                                                                      OracleConnectorConfig connectorConfig,
                                                                      Clock clock) {
        return new LogMinerStreamingChangeEventSourceMetrics(taskContext, queue, metadataProvider, connectorConfig, clock);
    }

    @Test
    public void testMetrics() {
        metrics.setLastCapturedDmlCount(1);
        assertThat(metrics.getTotalCapturedDmlCount() == 1).isTrue();

        metrics.setCurrentScn(Scn.valueOf(1000L));
        assertThat(metrics.getCurrentScn()).isEqualTo("1000");

        metrics.setLastDurationOfFetchQuery(Duration.ofMillis(100));
        assertThat(metrics.getLastDurationOfFetchQueryInMilliseconds()).isEqualTo(100);
        metrics.setLastDurationOfFetchQuery(Duration.ofMillis(200));
        assertThat(metrics.getLastDurationOfFetchQueryInMilliseconds()).isEqualTo(200);
        assertThat(metrics.getMaxDurationOfFetchQueryInMilliseconds()).isEqualTo(200);
        assertThat(metrics.getFetchingQueryCount()).isEqualTo(2);

        metrics.setCurrentLogFileNames(new HashSet<>(Arrays.asList("name", "name1")));
        assertThat(metrics.getCurrentRedoLogFileName()[0].equals("name")).isTrue();
        assertThat(metrics.getCurrentRedoLogFileName()[1].equals("name1")).isTrue();

        metrics.setMinedLogFileNames(new HashSet<>(Arrays.asList("arc1", "arc2", "online1")));
        assertThat(metrics.getMinedLogFileNames()).contains("arc1", "arc2", "online1");
        assertThat(metrics.getMinimumMinedLogCount()).isEqualTo(3);
        assertThat(metrics.getMaximumMinedLogCount()).isEqualTo(3);

        metrics.setMinedLogFileNames(new HashSet<>(Arrays.asList("arc2", "online1")));
        assertThat(metrics.getMinedLogFileNames()).contains("arc2", "online1");
        assertThat(metrics.getMinimumMinedLogCount()).isEqualTo(2);
        assertThat(metrics.getMaximumMinedLogCount()).isEqualTo(3);

        metrics.setSwitchCount(5);
        assertThat(metrics.getSwitchCounter() == 5).isTrue();

        metrics.reset();
        metrics.setLastDurationOfFetchQuery(Duration.ofMillis(1000));
        assertThat(metrics.getLastDurationOfFetchQueryInMilliseconds()).isEqualTo(1000);
        assertThat(metrics.getFetchingQueryCount()).isEqualTo(1);

        metrics.reset();
        metrics.setLastCapturedDmlCount(300);
        metrics.setLastBatchProcessingDuration(Duration.ofMillis(1000));
        assertThat(metrics.getLastCapturedDmlCount()).isEqualTo(300);
        assertThat(metrics.getLastBatchProcessingTimeInMilliseconds()).isEqualTo(1000);
        assertThat(metrics.getAverageBatchProcessingThroughput()).isGreaterThanOrEqualTo(300);
        assertThat(metrics.getMaxCapturedDmlInBatch()).isEqualTo(300);
        assertThat(metrics.getMaxBatchProcessingThroughput()).isEqualTo(300);

        metrics.setLastCapturedDmlCount(500);
        metrics.setLastBatchProcessingDuration(Duration.ofMillis(1000));
        assertThat(metrics.getAverageBatchProcessingThroughput()).isEqualTo(400);
        assertThat(metrics.getMaxCapturedDmlInBatch()).isEqualTo(500);
        assertThat(metrics.getMaxBatchProcessingThroughput()).isEqualTo(500);
        assertThat(metrics.getLastBatchProcessingThroughput()).isEqualTo(500);

        metrics.setLastBatchProcessingDuration(Duration.ofMillis(5000));
        assertThat(metrics.getLastBatchProcessingThroughput()).isEqualTo(100);

        metrics.setLastBatchProcessingDuration(Duration.ZERO);
        assertThat(metrics.getLastBatchProcessingThroughput()).isEqualTo(0);

        assertThat(metrics.getHoursToKeepTransactionInBuffer()).isEqualTo(0);
        assertThat(metrics.getMillisecondsToKeepTransactionsInBuffer()).isEqualTo(0L);

        metrics.setRedoLogStatuses(Collections.singletonMap("name", "current"));
        assertThat(metrics.getRedoLogStatus()[0].equals("name | current")).isTrue();

        assertThat(metrics.toString().contains("logMinerQueryCount"));
    }

    @Test
    public void testLagMetrics() {
        // no time difference between connector and database
        long lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(0);
        Instant dbEventTime = fixedClock.instant().minusMillis(2000);
        metrics.calculateLagFromSource(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(2000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(2000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(2000);

        // not realistic scenario
        dbEventTime = fixedClock.instant().plusMillis(3000);
        metrics.calculateLagFromSource(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(3000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(3000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(2000);

        metrics.reset();

        // ##########################
        // the database time is ahead 1s and has an offset of +12h
        OffsetDateTime dbTime = OffsetDateTime.parse("2021-05-16T00:30:01.00+12:00");
        metrics.setDatabaseTimeDifference(dbTime);

        dbEventTime = Instant.parse("2021-05-16T00:29:58.00Z");
        metrics.calculateLagFromSource(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(3000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(3000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(3000);

        dbEventTime = Instant.parse("2021-05-16T00:29:57.00Z");
        metrics.calculateLagFromSource(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(4000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(4000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(3000);

        metrics.reset();

        // ##########################
        // the database time is ahead 1s and has an offset of +0h (UTC)
        dbTime = OffsetDateTime.parse("2021-05-15T12:30:01.00Z");
        metrics.setDatabaseTimeDifference(dbTime);

        dbEventTime = Instant.parse("2021-05-15T12:29:58.00Z");
        metrics.calculateLagFromSource(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(3000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(3000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(3000);

        dbEventTime = Instant.parse("2021-05-15T12:29:57.00Z");
        metrics.calculateLagFromSource(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(4000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(4000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(3000);

        metrics.reset();

        // ##########################
        // the database time is ahead 1s and has an offset of -12h
        dbTime = OffsetDateTime.parse("2021-05-15T00:30:01.00-12:00");
        metrics.setDatabaseTimeDifference(dbTime);

        dbEventTime = Instant.parse("2021-05-15T00:29:58.00Z");
        metrics.calculateLagFromSource(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(3000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(3000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(3000);

        dbEventTime = Instant.parse("2021-05-15T00:29:57.00Z");
        metrics.calculateLagFromSource(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(4000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(4000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(3000);

        metrics.reset();

        // ##########################
        // the database time is behind 1s and has an offset of +12h
        dbTime = OffsetDateTime.parse("2021-05-16T00:29:59.00+12:00");
        metrics.setDatabaseTimeDifference(dbTime);

        dbEventTime = Instant.parse("2021-05-16T00:29:58.00Z");
        metrics.calculateLagFromSource(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(1000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(1000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(1000);

        // ##########################
        // the database time is behind 1s and has an offset of +0h (UTC)
        dbTime = OffsetDateTime.parse("2021-05-15T12:29:59.00Z");
        metrics.setDatabaseTimeDifference(dbTime);

        dbEventTime = Instant.parse("2021-05-15T12:29:58.00Z");
        metrics.calculateLagFromSource(dbEventTime);
        lag = metrics.getLagFromSourceInMilliseconds();
        assertThat(lag).isEqualTo(1000);
        assertThat(metrics.getMaxLagFromSourceInMilliseconds()).isEqualTo(1000);
        assertThat(metrics.getMinLagFromSourceInMilliseconds()).isEqualTo(1000);

        // ##########################
        // the database time is behind 1s and has an offset of -12h
        dbTime = OffsetDateTime.parse("2021-05-15T00:29:59.00-12:00");
        metrics.setDatabaseTimeDifference(dbTime);

        dbEventTime = Instant.parse("2021-05-15T00:29:58.00Z");
        metrics.calculateLagFromSource(dbEventTime);
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

        for (int i = 0; i < 1000; i++) {
            metrics.incrementTotalChangesCount();
            metrics.incrementCommittedTransactionCount();
        }
        assertThat(metrics.getRegisteredDmlCount()).isEqualTo(1000);
        assertThat(metrics.getNumberOfCommittedTransactions()).isEqualTo(1000);
        assertThat(metrics.getCommitThroughput()).isGreaterThanOrEqualTo(1_000);

        metrics.incrementOversizedTransactionCount();
        assertThat(metrics.getNumberOfOversizedTransactions()).isEqualTo(1);

        metrics.incrementRolledBackTransactionCount();
        assertThat(metrics.getNumberOfRolledBackTransactions()).isEqualTo(1);

        metrics.setActiveTransactionCount(5);
        assertThat(metrics.getNumberOfActiveTransactions()).isEqualTo(5);

        metrics.addRolledBackTransactionId("rolledback id");
        assertThat(metrics.getNumberOfRolledBackTransactions()).isEqualTo(1);
        assertThat(metrics.getRolledBackTransactionIds().contains("rolledback id")).isTrue();

        metrics.addAbandonedTransactionId("abandoned id");
        assertThat(metrics.getAbandonedTransactionIds().size()).isEqualTo(1);
        assertThat(metrics.getAbandonedTransactionIds().contains("abandoned id")).isTrue();

        metrics.setOldestScnDetails(Scn.valueOf(10L), null);
        assertThat(metrics.getOldestScn()).isEqualTo("10");

        metrics.setCommitScn(Scn.valueOf(10L));
        assertThat(metrics.getCommittedScn()).isEqualTo("10");

        assertThat(metrics.toString().contains("changesCount=1000")).isTrue();

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
        init(TestHelper.defaultConfig().with(OracleConnectorConfig.LOG_MINING_TRANSACTION_RETENTION_MS, 10_800_000));
        assertThat(metrics.getHoursToKeepTransactionInBuffer()).isEqualTo(3);
        assertThat(metrics.getMillisecondsToKeepTransactionsInBuffer()).isEqualTo(3 * 3600000);
    }

    @Test
    @FixFor("DBZ-5179")
    public void testRollbackTransactionIdSetSizeLimit() throws Exception {
        init(TestHelper.defaultConfig().with(OracleConnectorConfig.LOG_MINING_TRANSACTION_RETENTION_MS, 10_800_000));

        // Check state up to maximum size
        for (int i = 1; i <= 10; ++i) {
            metrics.addRolledBackTransactionId(String.valueOf(i));
        }
        assertThat(metrics.getRolledBackTransactionIds()).containsOnly("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");

        // Add another rollback transaction, does not exist in set
        metrics.addRolledBackTransactionId("11");
        assertThat(metrics.getRolledBackTransactionIds()).containsOnly("2", "3", "4", "5", "6", "7", "8", "9", "10", "11");

        // Add another rollback transaction, this time the same as before
        // Set should be unchanged.
        metrics.addRolledBackTransactionId("11");
        assertThat(metrics.getRolledBackTransactionIds()).containsOnly("2", "3", "4", "5", "6", "7", "8", "9", "10", "11");
    }

    @Test
    @FixFor("DBZ-5179")
    public void testAbandonedTransactionIdSetSizeLimit() throws Exception {
        init(TestHelper.defaultConfig().with(OracleConnectorConfig.LOG_MINING_TRANSACTION_RETENTION_MS, 10_800_000));

        // Check state up to maximum size
        for (int i = 1; i <= 10; ++i) {
            metrics.addAbandonedTransactionId(String.valueOf(i));
        }
        assertThat(metrics.getAbandonedTransactionIds()).containsOnly("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");

        // Add another abandoned transaction, does not exist in set
        metrics.addAbandonedTransactionId("11");
        assertThat(metrics.getAbandonedTransactionIds()).containsOnly("2", "3", "4", "5", "6", "7", "8", "9", "10", "11");

        // Add another abandoned transaction, this time the same as before
        // Set should be unchanged.
        metrics.addAbandonedTransactionId("11");
        assertThat(metrics.getAbandonedTransactionIds()).containsOnly("2", "3", "4", "5", "6", "7", "8", "9", "10", "11");
    }

}
