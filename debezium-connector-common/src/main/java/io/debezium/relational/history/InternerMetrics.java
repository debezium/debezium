/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.metrics.Metrics;
import io.debezium.util.InternerStats;

/**
 * JMX metrics for the schema object interner, registered under
 * {@code debezium.<connector>:type=connector-metrics,context=interner,server=<logical-name>}.
 *
 * <p>Lifecycle is tied to the surrounding {@link SchemaHistoryMetrics}: this bean is registered
 * when schema history starts and unregistered when it stops.</p>
 */
@ThreadSafe
public class InternerMetrics extends Metrics implements InternerMXBean {

    private static final String CONTEXT_NAME = "interner";

    private final InternerStats stats;

    public InternerMetrics(CommonConnectorConfig connectorConfig, boolean multiPartitionMode, InternerStats stats) {
        super(connectorConfig, CONTEXT_NAME, multiPartitionMode);
        this.stats = stats;
    }

    @Override
    public long getHitCount() {
        return stats.hitCount();
    }

    @Override
    public long getMissCount() {
        return stats.missCount();
    }

    @Override
    public int getPoolSize() {
        return stats.poolSize();
    }

    @Override
    public double getHitRatioPercent() {
        long hits = stats.hitCount();
        long total = hits + stats.missCount();
        return total == 0 ? 0.0 : hits * 100.0 / total;
    }
}
