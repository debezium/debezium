/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.concurrent.atomic.AtomicLong;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.jmx.BinaryLogClientStatistics;

/**
 * @author Randall Hauch
 */
class BinlogReaderMetrics extends Metrics implements BinlogReaderMetricsMXBean {

    private final BinaryLogClient client;
    private final BinaryLogClientStatistics stats;

    private final AtomicLong numberOfCommittedTransactions = new AtomicLong();
    private final AtomicLong numberOfRolledBackTransactions = new AtomicLong();
    private final AtomicLong numberOfNotWellFormedTransactions = new AtomicLong();
    private final AtomicLong numberOfLargeTransactions = new AtomicLong();

    public BinlogReaderMetrics( BinaryLogClient client) {
        super("binlog");
        this.client = client;
        this.stats = new BinaryLogClientStatistics(client);
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
    public String getLastEvent() {
        return this.stats.getLastEvent();
    }

    @Override
    public long getSecondsSinceLastEvent() {
        return this.stats.getSecondsSinceLastEvent();
    }

    @Override
    public long getSecondsBehindMaster() {
        return this.stats.getSecondsBehindMaster();
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

}
