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
    @FixFor("debezium/dbz#1438")
    public void shouldRestoreLastKnownTxIdAfterNonTransactionalMessage() throws Exception {
        final OffsetContext.Loader readOnlyLoader = new PostgresOffsetContext.Loader(new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.READ_ONLY_CONNECTION, true)
                .build()));
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 12345L);
        offsetValues.put(SourceInfo.TXID_KEY, 744L);

        final PostgresOffsetContext offsetContext = (PostgresOffsetContext) readOnlyLoader.load(offsetValues);
        assertThat(new HashMap<String, Object>(offsetContext.getOffset())).containsEntry(SourceInfo.TXID_KEY, 744L)
                .doesNotContainKey(PostgresOffsetContext.LAST_KNOWN_TXID_KEY);

        offsetContext.updateWalPosition(Lsn.valueOf(12400L), Lsn.valueOf(12400L), null, null, null, Operation.MESSAGE);
        final Map<String, Object> offsetAfterMessage = new HashMap<>(offsetContext.getOffset());
        assertThat(offsetAfterMessage).doesNotContainKey(SourceInfo.TXID_KEY)
                .containsEntry(PostgresOffsetContext.LAST_KNOWN_TXID_KEY, 744L);

        final PostgresOffsetContext restored = (PostgresOffsetContext) readOnlyLoader.load(offsetAfterMessage);
        assertThat(restored.lastKnownTxId()).isEqualTo(744L);
    }

    @Test
    @FixFor("debezium/dbz#1438")
    public void shouldNotStoreLastKnownTxIdWithoutReadOnlyConnection() throws Exception {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 12345L);
        offsetValues.put(SourceInfo.TXID_KEY, 744L);

        final PostgresOffsetContext offsetContext = (PostgresOffsetContext) offsetLoader.load(offsetValues);
        offsetContext.updateWalPosition(Lsn.valueOf(12400L), Lsn.valueOf(12400L), null, null, null, Operation.MESSAGE);

        assertThat(new HashMap<String, Object>(offsetContext.getOffset())).doesNotContainKey(SourceInfo.TXID_KEY)
                .doesNotContainKey(PostgresOffsetContext.LAST_KNOWN_TXID_KEY);
    }
}
