/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.SharedSessionContract;
import org.hibernate.Transaction;
import org.hibernate.exception.LockAcquisitionException;
import org.hibernate.exception.SQLGrammarException;
import org.hibernate.jdbc.ReturningWork;
import org.hibernate.query.NativeQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.doc.FixFor;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.field.FieldDescriptor;
import io.debezium.sink.spi.SinkProgressListener;

/**
 * Unit tests for the schema evolution recovery in {@link DefaultRecordWriter}.
 *
 * The session and dialect are mocked because the tests need to fail specific metadata reads and
 * ALTER statements at exact points, which is not possible to reproduce deterministically with a
 * real database.
 */
@Tag("UnitTests")
class DefaultRecordWriterTest extends AbstractBaseJdbcSinkTest {

    private static final String ALTER_SQL = "ALTER TABLE test_table ADD order_status varchar";

    private final CollectionId collectionId = new CollectionId(null, null, "test_table");
    private final TableDescriptor tableWithoutColumn = TableDescriptor.builder().tableName("test_table").build();
    private final TableDescriptor tableWithColumn = TableDescriptor.builder()
            .tableName("test_table")
            .column(ColumnDescriptor.builder()
                    .columnName("order_status")
                    .jdbcType(Types.VARCHAR)
                    .typeName("varchar")
                    .build())
            .build();

    private SharedSessionContract session;
    private DatabaseDialect dialect;
    private JdbcSinkRecord record;
    private NativeQuery<Object> alterQuery;
    private DefaultRecordWriter writer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws SQLException {
        final Connection connection = mock(Connection.class);
        session = mock(SharedSessionContract.class);
        when(session.doReturningWork(any())).thenAnswer(invocation -> ((ReturningWork<?>) invocation.getArgument(0)).execute(connection));
        when(session.beginTransaction()).thenReturn(mock(Transaction.class));
        alterQuery = mock(NativeQuery.class);
        when(session.createNativeQuery(anyString(), eq(Object.class))).thenReturn(alterQuery);

        dialect = mock(DatabaseDialect.class);
        when(dialect.tableExists(any(), eq(collectionId))).thenReturn(true);
        when(dialect.resolveMissingFields(any(), same(tableWithoutColumn))).thenReturn(Set.of("order_status"));
        when(dialect.resolveMissingFields(any(), same(tableWithColumn))).thenReturn(Set.of());
        when(dialect.getAlterTableStatement(any(), any(), any())).thenReturn(ALTER_SQL);

        record = mock(JdbcSinkRecord.class);
        when(record.allFields()).thenReturn(Map.of("order_status", new FieldDescriptor(Schema.OPTIONAL_STRING_SCHEMA, "order_status", false)));

        final JdbcSinkConnectorConfig config = getConfig(Map.of(
                JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue(),
                JdbcSinkConnectorConfig.FLUSH_MAX_RETRIES, "2",
                JdbcSinkConnectorConfig.FLUSH_RETRY_DELAY_MS, "1"));
        writer = new DefaultRecordWriter(session, new QueryBinderResolver(), config, dialect, SinkProgressListener.NO_OP());
    }

    @Test
    @FixFor("debezium/dbz#3067")
    void shouldRetryTableReadThatLostDeadlockToConcurrentAlter() throws SQLException {
        when(dialect.readTable(any(), eq(collectionId)))
                .thenThrow(deadlock())
                .thenReturn(tableWithColumn);

        assertThat(writer.checkAndApplyTableChangesIfNeeded(collectionId, record)).isSameAs(tableWithColumn);

        verify(dialect, times(2)).readTable(any(), eq(collectionId));
        verify(session, never()).createNativeQuery(anyString(), eq(Object.class));
    }

    @Test
    @FixFor("debezium/dbz#3067")
    void shouldRetryRecheckReadThatDeadlocksAfterFailedAlter() throws SQLException {
        when(dialect.readTable(any(), eq(collectionId)))
                .thenReturn(tableWithoutColumn)
                .thenThrow(deadlock())
                .thenReturn(tableWithColumn);
        when(alterQuery.executeUpdate()).thenThrow(columnAlreadyExists());

        assertThat(writer.checkAndApplyTableChangesIfNeeded(collectionId, record)).isSameAs(tableWithColumn);

        verify(alterQuery, times(1)).executeUpdate();
    }

    @Test
    @FixFor("debezium/dbz#3067")
    void shouldFailWithOriginalAlterFailureAttachedWhenDeadlockRetriesAreExhausted() throws SQLException {
        final SQLGrammarException alterFailure = columnAlreadyExists();
        when(dialect.readTable(any(), eq(collectionId)))
                .thenReturn(tableWithoutColumn).thenThrow(deadlock())
                .thenReturn(tableWithoutColumn).thenThrow(deadlock())
                .thenReturn(tableWithoutColumn).thenThrow(deadlock());
        when(alterQuery.executeUpdate()).thenThrow(alterFailure);

        assertThatThrownBy(() -> writer.checkAndApplyTableChangesIfNeeded(collectionId, record))
                .isInstanceOf(LockAcquisitionException.class)
                .satisfies(e -> assertThat(e.getSuppressed()).contains(alterFailure));

        // initial attempt plus two retries
        verify(alterQuery, times(3)).executeUpdate();
    }

    @Test
    @FixFor("debezium/dbz#3067")
    void shouldNotRetryTableReadFailureThatIsNotLockConflict() throws SQLException {
        final SQLException failure = new SQLException("permission denied");
        when(dialect.readTable(any(), eq(collectionId))).thenThrow(failure);

        assertThatThrownBy(() -> writer.checkAndApplyTableChangesIfNeeded(collectionId, record)).isSameAs(failure);

        verify(dialect, times(1)).readTable(any(), eq(collectionId));
    }

    @Test
    @FixFor("debezium/dbz#2302")
    void shouldRetryAlterOnceWhenColumnIsStillMissingAfterFailedAlter() throws SQLException {
        when(dialect.readTable(any(), eq(collectionId)))
                .thenReturn(tableWithoutColumn, tableWithoutColumn, tableWithoutColumn, tableWithColumn);
        final SQLGrammarException alterFailure = columnAlreadyExists();
        when(alterQuery.executeUpdate()).thenThrow(alterFailure).thenReturn(0);

        assertThat(writer.checkAndApplyTableChangesIfNeeded(collectionId, record)).isSameAs(tableWithColumn);

        verify(alterQuery, times(2)).executeUpdate();
    }

    @Test
    @FixFor("debezium/dbz#2302")
    void shouldKeepOriginalAlterFailureWhenRetriedAlterFails() throws SQLException {
        when(dialect.readTable(any(), eq(collectionId))).thenReturn(tableWithoutColumn);
        final SQLGrammarException alterFailure = columnAlreadyExists();
        final SQLGrammarException retryFailure = columnAlreadyExists();
        when(alterQuery.executeUpdate()).thenThrow(alterFailure).thenThrow(retryFailure);

        assertThatThrownBy(() -> writer.checkAndApplyTableChangesIfNeeded(collectionId, record))
                .isSameAs(retryFailure)
                .satisfies(e -> assertThat(e.getSuppressed()).contains(alterFailure));

        verify(alterQuery, times(2)).executeUpdate();
    }

    private static LockAcquisitionException deadlock() {
        return new LockAcquisitionException("Error executing work",
                new SQLException("Transaction was deadlocked on lock resources with another process and has been chosen as the deadlock victim.",
                        "40001", 1205));
    }

    private static SQLGrammarException columnAlreadyExists() {
        return new SQLGrammarException("Column names in each table must be unique.", new SQLException("Column already exists", "S0001", 2705));
    }
}
