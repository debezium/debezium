/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.metrics;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.jmx.BinaryLogClientStatistics;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.binlog.BinlogDatabaseSchema;
import io.debezium.connector.binlog.BinlogTaskContext;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

/**
 * Tracks the streaming metrics for binlog-based connectors.
 *
 * @author Chris Cranford
 */
public class BinlogStreamingChangeEventSourceMetrics<T extends BinlogDatabaseSchema, P extends Partition>
        extends DefaultStreamingChangeEventSourceMetrics<P>
        implements BinlogStreamingChangeEventSourceMetricsMXBean {

    private final BinaryLogClient client;
    private final BinaryLogClientStatistics stats;
    private final T schema;

    private final AtomicLong numberOfCommittedTransactions = new AtomicLong();
    private final AtomicLong numberOfRolledBackTransactions = new AtomicLong();
    private final AtomicLong numberOfNotWellFormedTransactions = new AtomicLong();
    private final AtomicLong numberOfLargeTransactions = new AtomicLong();
    private final AtomicBoolean isGtidModeEnabled = new AtomicBoolean(false);
    private final AtomicLong milliSecondsBehindMaster = new AtomicLong();
    private final AtomicReference<String> lastTransactionId = new AtomicReference<>();

    public BinlogStreamingChangeEventSourceMetrics(BinlogTaskContext<T> taskContext,
                                                   ChangeEventQueueMetrics changeEventQueueMetrics,
                                                   EventMetadataProvider eventMetadataProvider) {
        super(taskContext, changeEventQueueMetrics, eventMetadataProvider);
        this.client = taskContext.getBinaryLogClient();
        this.stats = new BinaryLogClientStatistics(client);
        this.schema = taskContext.getSchema();
        this.milliSecondsBehindMaster.set(-1);
    }

    @Override
    public boolean isConnected() {
        return this.client.isConnected();
    }

    @Override
    public String getBinlogFilename() {
        return this.client.getBinlogFilename();
    }

    @Override
    public long getBinlogPosition() {
        return this.client.getBinlogPosition();
    }

    @Override
    public String getGtidSet() {
        return this.client.getGtidSet();
    }

    @Override
    public boolean getIsGtidModeEnabled() {
        return isGtidModeEnabled.get();
    }

    @Override
    public String getLastEvent() {
        return this.stats.getLastEvent();
    }

    @Override
    public long getMilliSecondsSinceLastEvent() {
        return this.stats.getSecondsSinceLastEvent() * 1000;
    }

    @Override
    public long getTotalNumberOfEventsSeen() {
        return this.stats.getTotalNumberOfEventsSeen();
    }

    @Override
    public long getNumberOfSkippedEvents() {
        return this.stats.getNumberOfSkippedEvents();
    }

    @Override
    public long getNumberOfDisconnects() {
        return this.stats.getNumberOfDisconnects();
    }

    @Override
    public void reset() {
        this.stats.reset();
        numberOfCommittedTransactions.set(0);
        numberOfRolledBackTransactions.set(0);
        numberOfNotWellFormedTransactions.set(0);
        numberOfLargeTransactions.set(0);
        lastTransactionId.set(null);
        isGtidModeEnabled.set(false);
    }

    @Override
    public long getNumberOfCommittedTransactions() {
        return numberOfCommittedTransactions.get();
    }

    @Override
    public long getNumberOfRolledBackTransactions() {
        return numberOfRolledBackTransactions.get();
    }

    @Override
    public long getNumberOfNotWellFormedTransactions() {
        return numberOfNotWellFormedTransactions.get();
    }

    @Override
    public long getNumberOfLargeTransactions() {
        return numberOfLargeTransactions.get();
    }

    @Override
    public String[] getCapturedTables() {
        return schema.capturedTablesAsStringArray();
    }

    @Override
    public long getMilliSecondsBehindSource() {
        return milliSecondsBehindMaster.get();
    }

    @Override
    public Map<String, String> getSourceEventPosition() {
        return Collect.hashMapOf(
                "filename", getBinlogFilename(),
                "position", Long.toString(getBinlogPosition()),
                "gtid", getGtidSet());
    }

    @Override
    public String getLastTransactionId() {
        return lastTransactionId.get();
    }

    public void onCommittedTransaction() {
        numberOfCommittedTransactions.incrementAndGet();
    }

    public void onRolledBackTransaction() {
        numberOfRolledBackTransactions.incrementAndGet();
    }

    public void onNotWellFormedTransaction() {
        numberOfNotWellFormedTransactions.incrementAndGet();
    }

    public void onLargeTransaction() {
        numberOfLargeTransactions.incrementAndGet();
    }

    public void onGtidChange(String gtid) {
        lastTransactionId.set(gtid);
    }

    public void setIsGtidModeEnabled(final boolean enabled) {
        isGtidModeEnabled.set(enabled);
    }

    public void setMilliSecondsBehindSource(long value) {
        milliSecondsBehindMaster.set(value);
    }

}
