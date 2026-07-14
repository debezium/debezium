/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.hibernate.StatelessSession;
import org.hibernate.dialect.DatabaseVersion;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.InsertMode;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.connector.jdbc.util.DebeziumSinkRecordFactory;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.metadata.CollectionId;
import io.debezium.openlineage.ConnectorContext;
import io.debezium.sink.spi.SinkProgressListener;

@Tag("UnitTests")
class JdbcChangeEventSinkTest {

    private static final SinkRecordFactory RECORD_FACTORY = new DebeziumSinkRecordFactory();

    @ParameterizedTest
    @EnumSource(InsertMode.class)
    void shouldReportWriteOperationUsingConfiguredInsertMode(InsertMode insertMode) throws Exception {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(
                JdbcSinkConnectorConfig.INSERT_MODE, insertMode.getValue()));
        final CollectionId collectionId = new CollectionId(null, null, "database_schema_table");
        final TableDescriptor table = TableDescriptor.builder()
                .tableName("database_schema_table")
                .build();
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        final DatabaseVersion databaseVersion = DatabaseVersion.make(1);
        final StatelessSession session = mock(StatelessSession.class);
        final SinkProgressListener progressListener = mock(SinkProgressListener.class);
        final DefaultRecordWriter recordWriter = mock(DefaultRecordWriter.class);
        final ConnectorContext connectorContext = new ConnectorContext("jdbc-sink", "jdbc", "0", "test", UUID.randomUUID(), Map.of());

        when(dialect.getVersion()).thenReturn(databaseVersion);
        when(dialect.getCollectionId("database_schema_table")).thenReturn(collectionId);
        when(dialect.resolveMissingFields(any(), any())).thenReturn(Set.of());
        when(recordWriter.checkAndApplyTableChangesIfNeeded(any(), any())).thenReturn(table);
        when(recordWriter.executeWithRetries(anyString(), any())).thenAnswer(invocation -> {
            final Callable<?> callable = invocation.getArgument(1);
            return callable.call();
        });
        when(recordWriter.getConfig()).thenReturn(config);
        when(recordWriter.progressListener()).thenReturn(progressListener);
        doCallRealMethod().when(recordWriter).processMetrics(any(), any(RecordWriter.SqlStatementInfo.class));

        doAnswer(invocation -> {
            List<JdbcSinkRecord> records = invocation.getArgument(1);
            recordWriter.processMetrics(records, new RecordWriter.SqlStatementInfo("UPDATE;", true, false));
            return null;
        }).when(recordWriter).write(any(TableDescriptor.class), any());

        final JdbcChangeEventSink sink = new JdbcChangeEventSink(config, session, dialect, recordWriter, connectorContext, progressListener);
        sink.execute(List.of(RECORD_FACTORY.createRecord("database.schema.table", config).getOriginalKafkaRecord()));
        sink.forceFlush();

        verify(progressListener).written(1);
        verifyNoMoreInteractions(progressListener);
    }

}
