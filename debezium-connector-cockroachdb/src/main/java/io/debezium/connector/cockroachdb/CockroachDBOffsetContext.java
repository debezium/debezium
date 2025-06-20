/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;

/**
 * Represents the current offset state for the CockroachDB changefeed source.
 * Stores resolved timestamp, HLC (Hybrid Logical Clock), and nanosecond timestamp.
 *
 * @author Virag Tripathi
 */
public class CockroachDBOffsetContext implements OffsetContext {

    private static final String FIELD_TIMESTAMP = "resolved_timestamp";
    private static final String FIELD_HLC = "ts_hlc";
    private static final String FIELD_TS_NS = "ts_ns";

    private final CockroachDBConnectorConfig connectorConfig;
    private final Instant resolvedTimestamp;
    private final String hlc;
    private final Long tsNs;

    public CockroachDBOffsetContext(CockroachDBConnectorConfig connectorConfig, Instant resolvedTimestamp, String hlc, Long tsNs) {
        this.connectorConfig = connectorConfig;
        this.resolvedTimestamp = resolvedTimestamp;
        this.hlc = hlc;
        this.tsNs = tsNs;
    }

    public static CockroachDBOffsetContext initial(CockroachDBConnectorConfig connectorConfig) {
        return new CockroachDBOffsetContext(connectorConfig, Clock.system().currentTimeAsInstant(), null, null);
    }

    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(FIELD_TIMESTAMP, resolvedTimestamp.toString());
        if (hlc != null) {
            offset.put(FIELD_HLC, hlc);
        }
        if (tsNs != null) {
            offset.put(FIELD_TS_NS, tsNs);
        }
        return offset;
    }

    public Instant getResolvedTimestamp() {
        return resolvedTimestamp;
    }

    public String getHlc() {
        return hlc;
    }

    public Long getTsNs() {
        return tsNs;
    }

    @Override
    public String toString() {
        return "CockroachDBOffsetContext{" +
                "resolvedTimestamp=" + resolvedTimestamp +
                ", ts_hlc=" + hlc +
                ", ts_ns=" + tsNs +
                '}';
    }

    public static class Loader implements OffsetContext.Loader<CockroachDBOffsetContext> {
        private final CockroachDBConnectorConfig connectorConfig;

        public Loader(CockroachDBConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public CockroachDBOffsetContext load(Map<String, ?> offset) {
            String ts = (String) offset.get(FIELD_TIMESTAMP);
            Instant resolvedTs = ts != null ? Instant.parse(ts) : Clock.system().currentTimeAsInstant();
            String hlc = (String) offset.get(FIELD_HLC);
            Long tsNs = offset.containsKey(FIELD_TS_NS) ? ((Number) offset.get(FIELD_TS_NS)).longValue() : null;
            return new CockroachDBOffsetContext(connectorConfig, resolvedTs, hlc, tsNs);
        }
    }
}
