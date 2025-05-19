/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.metrics.Metrics;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;

/**
 * Implementation of {@link DatabaseSchema} metrics.
 *
 * @author Jiri Pechanec
 *
 */
public class SchemaHistoryMetrics extends Metrics implements SchemaHistoryListener, SchemaHistoryMXBean {

    private static final String CONTEXT_NAME = "schema-history";

    private static final Duration PAUSE_BETWEEN_LOG_MESSAGES = Duration.ofSeconds(2);

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaHistoryMetrics.class);

    public enum SchemaHistoryStatus {
        STOPPED,
        RECOVERING,
        RUNNING
    }

    private SchemaHistoryStatus status = SchemaHistoryStatus.STOPPED;
    private Instant recoveryStartTime = null;
    private AtomicLong changesRecovered = new AtomicLong();
    private AtomicLong totalChangesApplied = new AtomicLong();
    private Instant lastChangeAppliedTimestamp;
    private Instant lastChangeRecoveredTimestamp;
    private HistoryRecord lastAppliedChange;
    private HistoryRecord lastRecoveredChange;
    private final Clock clock = Clock.system();
    private final ElapsedTimeStrategy lastChangeAppliedLogDelay = ElapsedTimeStrategy.constant(clock, PAUSE_BETWEEN_LOG_MESSAGES);
    private final ElapsedTimeStrategy lastChangeRecoveredLogDelay = ElapsedTimeStrategy.constant(clock, PAUSE_BETWEEN_LOG_MESSAGES);

    public SchemaHistoryMetrics(CommonConnectorConfig connectorConfig, boolean multiPartitionMode) {
        super(connectorConfig, CONTEXT_NAME, multiPartitionMode);
        lastChangeAppliedLogDelay.hasElapsed();
        lastChangeRecoveredLogDelay.hasElapsed();
    }

    @Override
    public long getStatus() {
        if (status == SchemaHistoryStatus.STOPPED) {
            return 0;
        }
        else if (status == SchemaHistoryStatus.RECOVERING) {
            return 1;
        }
        else {
            return 2;
        }
    }

    @Override
    public long getRecoveryStartTime() {
        return recoveryStartTime == null ? -1 : recoveryStartTime.getEpochSecond();
    }

    @Override
    public long getChangesRecovered() {
        return changesRecovered.get();
    }

    @Override
    public long getChangesApplied() {
        return totalChangesApplied.get();
    }

    @Override
    public long getMilliSecondsSinceLastAppliedChange() {
        return lastChangeAppliedTimestamp == null ? -1 : Duration.between(lastChangeAppliedTimestamp, Instant.now()).toMillis();
    }

    @Override
    public long getMilliSecondsSinceLastRecoveredChange() {
        return lastChangeRecoveredTimestamp == null ? -1 : Duration.between(lastChangeRecoveredTimestamp, Instant.now()).toMillis();
    }

    @Override
    public String getLastAppliedChange() {
        return lastAppliedChange == null ? "" : lastAppliedChange.toString();
    }

    @Override
    public String getLastRecoveredChange() {
        return lastRecoveredChange == null ? "" : lastRecoveredChange.toString();
    }

    @Override
    public void started() {
        status = SchemaHistoryStatus.RUNNING;
        register();
    }

    @Override
    public void stopped() {
        status = SchemaHistoryStatus.STOPPED;
        unregister();
    }

    @Override
    public void recoveryStarted() {
        status = SchemaHistoryStatus.RECOVERING;
        recoveryStartTime = Instant.now();
        LOGGER.info("Started database schema history recovery");
    }

    @Override
    public void recoveryStopped() {
        status = SchemaHistoryStatus.RUNNING;
        LOGGER.info("Finished database schema history recovery of {} change(s) in {} ms", changesRecovered.get(),
                Duration.between(recoveryStartTime, Instant.now()).toMillis());
    }

    @Override
    public void onChangeFromHistory(HistoryRecord record) {
        lastRecoveredChange = record;
        changesRecovered.incrementAndGet();
        if (lastChangeRecoveredLogDelay.hasElapsed()) {
            LOGGER.info("Database schema history recovery in progress, recovered {} records", changesRecovered);
        }
        lastChangeRecoveredTimestamp = Instant.now();
    }

    @Override
    public void onChangeApplied(HistoryRecord record) {
        lastAppliedChange = record;
        totalChangesApplied.incrementAndGet();
        if (lastChangeAppliedLogDelay.hasElapsed()) {
            LOGGER.info("Already applied {} database changes", totalChangesApplied);
        }
        lastChangeAppliedTimestamp = Instant.now();
    }

}
