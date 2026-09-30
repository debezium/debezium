/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Operation;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.spi.OffsetContext;

/**
 * @author vjuranek
 */
public class PostgresOffsetContextTest {

    private PostgresConnectorConfig connectorConfig;
    private OffsetContext.Loader offsetLoader;

    @BeforeEach
    void beforeEach() throws Exception {
        this.connectorConfig = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        this.offsetLoader = new PostgresOffsetContext.Loader(this.connectorConfig);
    }

    @Test
    @FixFor("DBZ-5070")
    public void shouldNotResetLsnWhenLastCommitLsnIsNull() throws Exception {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 12345L);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        offsetValues.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, null);

        final PostgresOffsetContext offsetContext = (PostgresOffsetContext) offsetLoader.load(offsetValues);
        assertThat(offsetContext.lsn()).isEqualTo(Lsn.valueOf(12345L));
    }

    @Test
    @FixFor("debezium/dbz#2549")
    public void shouldRetainOffsetFromBeforeCurrentSourceEvent() {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 12345L);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);

        final PostgresOffsetContext offsetContext = (PostgresOffsetContext) offsetLoader.load(offsetValues);
        offsetContext.markSourceEventStarted();
        offsetContext.updateWalPosition(Lsn.valueOf(23456L), Lsn.valueOf(23456L), null, null, null, Operation.UPDATE);

        assertThat(offsetContext.getOffset().get(SourceInfo.LSN_KEY)).isEqualTo(23456L);
        assertThat(offsetContext.getOffsetForIncompleteEvent().get(SourceInfo.LSN_KEY)).isEqualTo(12345L);
    }
}
