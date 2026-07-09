/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.relational.TableId;

/**
 * Unit tests for {@link TiDbOffsetContext}.
 *
 * @author Aviral Srivastava
 */
public class TiDbOffsetContextTest {

    private static TiDbConnectorConfig connectorConfig() {
        return new TiDbConnectorConfig(Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "tidb_server")
                .with(TiDbConnectorConfig.TICDC_BOOTSTRAP_SERVERS, "localhost:9092")
                .with(TiDbConnectorConfig.TICDC_TOPICS, "ticdc-inventory")
                .build());
    }

    @Test
    public void shouldStartWithoutStreamPosition() {
        final TiDbOffsetContext offsetContext = TiDbOffsetContext.empty(connectorConfig());
        assertThat(offsetContext.hasStreamPosition()).isFalse();
        assertThat(offsetContext.nextOffsetFor("ticdc-inventory", 0)).isEmpty();
    }

    @Test
    public void shouldRoundTripOffsets() {
        final TiDbConnectorConfig config = connectorConfig();
        final TiDbOffsetContext offsetContext = TiDbOffsetContext.empty(config);

        offsetContext.event(new TableId("inventory", null, "products"), Instant.ofEpochMilli(1717000000000L),
                446245805252059137L, "tidb-cluster-1", "ticdc-inventory", 0, 42L);
        offsetContext.event(new TableId("inventory", null, "orders"), Instant.ofEpochMilli(1717000001000L),
                446245805252059138L, "tidb-cluster-1", "ticdc-inventory", 1, 7L);

        final Map<String, ?> offset = offsetContext.getOffset();
        final TiDbOffsetContext loaded = new TiDbOffsetContext.Loader(config).load(offset);

        assertThat(loaded.getCommitTs()).isEqualTo(446245805252059138L);
        assertThat(loaded.hasStreamPosition()).isTrue();
        assertThat(loaded.nextOffsetFor("ticdc-inventory", 0)).hasValue(42L);
        assertThat(loaded.nextOffsetFor("ticdc-inventory", 1)).hasValue(7L);
        assertThat(loaded.nextOffsetFor("ticdc-inventory", 2)).isEmpty();
    }
}
