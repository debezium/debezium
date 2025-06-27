/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

/**
 *
 * @author Virag Tripathi
 */
class CockroachDBOffsetContextTest {

    private static final String MOCK_HLC = "486481203757654017";
    private static final Long MOCK_TS_NS = 1718700000000000000L;
    private static final Instant MOCK_INSTANT = Instant.parse("2025-06-18T17:15:00.000Z");

    private final CockroachDBConnectorConfig config = new CockroachDBConnectorConfig(
            Configuration.create().with("name", "test-connector").build());

    @Test
    void testOffsetSerialization() {
        CockroachDBOffsetContext ctx = new CockroachDBOffsetContext(config, MOCK_INSTANT, MOCK_HLC, MOCK_TS_NS);
        Map<String, ?> offset = ctx.getOffset();

        assertThat(offset.get("resolved_timestamp")).isEqualTo("2025-06-18T17:15:00Z");
        assertThat(offset.get("ts_hlc")).isEqualTo(MOCK_HLC);
        assertThat(offset.get("ts_ns")).isEqualTo(MOCK_TS_NS);
    }

    @Test
    void testOffsetDeserialization() {
        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put("resolved_timestamp", "2025-06-18T17:15:00Z");
        offsetMap.put("ts_hlc", MOCK_HLC);
        offsetMap.put("ts_ns", MOCK_TS_NS);

        CockroachDBOffsetContext.Loader loader = new CockroachDBOffsetContext.Loader(config);
        CockroachDBOffsetContext ctx = loader.load(offsetMap);

        assertThat(ctx.getResolvedTimestamp()).isEqualTo(MOCK_INSTANT);
        assertThat(ctx.getHlc()).isEqualTo(MOCK_HLC);
        assertThat(ctx.getTsNs()).isEqualTo(MOCK_TS_NS);
    }

    @Test
    void testOffsetDeserializationFallback() {
        Map<String, Object> offsetMap = new HashMap<>();
        // Intentionally missing keys
        CockroachDBOffsetContext.Loader loader = new CockroachDBOffsetContext.Loader(config);
        CockroachDBOffsetContext ctx = loader.load(offsetMap);

        assertThat(ctx.getResolvedTimestamp()).isNotNull();
        assertThat(ctx.getHlc()).isNull();
        assertThat(ctx.getTsNs()).isNull();
    }
}
