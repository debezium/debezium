/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.spi.OffsetContext;

/**
 * @author vjuranek
 */
public class PostgresOffsetContextTest {

    private PostgresConnectorConfig connectorConfig;
    private OffsetContext.Loader offsetLoader;

    @Before
    public void beforeEach() throws Exception {
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
}
