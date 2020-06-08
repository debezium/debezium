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
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.metrics.Metrics;
import io.debezium.schema.DatabaseSchema;

/**
 * Implementation of {@link DatabaseSchema} metrics.
 *
 * @author Jiri Pechanec
 *
 */
public class DatabaseHistoryMetrics extends Metrics implements DatabaseHistoryListener, DatabaseHistoryMXBean {

    private static final String CONTEXT_NAME = "schema-history";

    private static final Duration PAUSE_BETWEEN_LOG_MESSAGES = Duration.ofSeconds(2);

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseHistoryMetrics.class);

    public static enum DatabaseHistoryStatus {
        STOPPED,
        RECOVERING,
        RUNNING
    }

    private DatabaseHistoryStatus status = DatabaseHistoryStatus.STOPPED;
    private Instant recoveryStartTime = null;
    private AtomicLong changesRecovered = new AtomicLong();
    private AtomicLong totalChangesApplied = new AtomicLong();
    private Instant lastChangeAppliedTimestamp;
    private Instant lastChangeRecoveredTimestamp;
    private HistoryRecord lastAppliedChange;
    private HistoryRecord lastRecoveredChange;

    protected <T extends CdcSourceTaskContext> DatabaseHistoryMetrics(T taskContext, String contextName) {
        super(taskContext, contextName);
    }

    public DatabaseHistoryMetrics(CommonConnectorConfig connectorConfig) {
        super(connectorConfig, CONTEXT_NAME);
    }

    @Override
    public String getStatus() {
        return status.toString();
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
        status = DatabaseHistoryStatus.RUNNING;
        register(LOGGER);
    }

    @Override
    public void stopped() {
        status = DatabaseHistoryStatus.STOPPED;
        unregister(LOGGER);
    }

    @Override
    public void recoveryStarted() {
        status = DatabaseHistoryStatus.RECOVERING;
        recoveryStartTime = Instant.now();
        LOGGER.info("Started database history recovery");
    }

    @Override
    public void recoveryStopped() {
        status = DatabaseHistoryStatus.RUNNING;
        LOGGER.info("Finished database history recovery of {} change(s) in {} ms", changesRecovered.get(), Duration.between(recoveryStartTime, Instant.now()).toMillis());
    }

    @Override
    public void onChangeFromHistory(HistoryRecord record) {
        lastRecoveredChange = record;
        changesRecovered.incrementAndGet();
        if (getMilliSecondsSinceLastRecoveredChange() >= PAUSE_BETWEEN_LOG_MESSAGES.toMillis()) {
            LOGGER.info("Database history recovery in progress, recovered {} records", changesRecovered);
        }
        lastChangeRecoveredTimestamp = Instant.now();
    }

    @Override
    public void onChangeApplied(HistoryRecord record) {
        lastAppliedChange = record;
        totalChangesApplied.incrementAndGet();
        if (getMilliSecondsSinceLastAppliedChange() >= PAUSE_BETWEEN_LOG_MESSAGES.toMillis()) {
            LOGGER.info("Already applied {} database changes", totalChangesApplied);
        }
        lastChangeAppliedTimestamp = Instant.now();
    }

}
