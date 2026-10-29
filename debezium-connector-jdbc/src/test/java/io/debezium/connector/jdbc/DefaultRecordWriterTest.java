/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;

import org.hibernate.SharedSessionContract;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.util.DebeziumSinkRecordFactory;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.batch.Batch;
import io.debezium.sink.batch.BatchRecord;
import io.debezium.sink.spi.SinkProgressListener;

@Tag("UnitTests")
class DefaultRecordWriterTest extends AbstractBaseJdbcSinkTest {

    private static final SinkRecordFactory RECORD_FACTORY = new DebeziumSinkRecordFactory();

    @Test
    @FixFor("debezium/dbz#2916")
    void resolveBatchByTableShouldSkipTombstonesForMissingTable() throws Exception {
        final JdbcSinkConnectorConfig config = getConfig(Map.of(
                JdbcSinkConnectorConfig.DELETE_ENABLED, "true",
                JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, "record_key"));
        final CollectionId collectionId = new CollectionId(null, null, "database_schema_table");
        final SinkProgressListener progressListener = mock(SinkProgressListener.class);
        final DefaultRecordWriter writer = spy(new DefaultRecordWriter(mock(SharedSessionContract.class), new QueryBinderResolver(), config,
                mock(DatabaseDialect.class), progressListener));

        doReturn(false).when(writer).tableExists(collectionId);

        final Batch batch = new Batch(List.of(
                new BatchRecord(collectionId, RECORD_FACTORY.tombstoneRecord("database.schema.table", config)),
                new BatchRecord(collectionId, RECORD_FACTORY.tombstoneRecord("database.schema.table", config))));

        assertThat(writer.resolveBatchByTable(batch)).isEmpty();

        // The table must not be created from a tombstone, which has no value to derive the columns from
        verify(writer, never()).checkAndApplyTableChangesIfNeeded(any(), any());
        // The existence of the table is checked only once per batch
        verify(writer).tableExists(collectionId);
        verify(progressListener, times(2)).filtered();
    }
}
