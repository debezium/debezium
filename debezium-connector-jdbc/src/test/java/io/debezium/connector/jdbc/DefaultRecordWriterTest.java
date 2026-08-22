/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.SharedSessionContract;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.field.JdbcFieldDescriptor;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.connector.jdbc.type.connect.ConnectBytesType;
import io.debezium.connector.jdbc.util.BinaryHandling;
import io.debezium.doc.FixFor;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.field.FieldDescriptor;
import io.debezium.sink.spi.SinkProgressListener;

@Tag("UnitTests")
class DefaultRecordWriterTest extends AbstractBaseJdbcSinkTest {

    private static final String ENCODED_TOPIC = "encoded";
    private static final String RAW_TOPIC = "raw";

    @Test
    @FixFor("debezium/dbz#2468")
    void performTableWritesShouldPartitionAlternatingBindingModesIntoContiguousGroups() throws SQLException {
        final DatabaseDialect dialect = binaryHandlingDialect();
        final DefaultRecordWriter writer = writerSpy(dialect, binaryHandlingEnabledConfig());

        final JdbcSinkRecord encoded1 = bytesRecord(ENCODED_TOPIC);
        final JdbcSinkRecord raw = bytesRecord(RAW_TOPIC);
        final JdbcSinkRecord encoded2 = bytesRecord(ENCODED_TOPIC);

        writer.performTableWrites(mock(Connection.class), table(), List.of(encoded1, raw, encoded2));

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<List<JdbcSinkRecord>> groups = ArgumentCaptor.forClass(List.class);
        verify(writer, times(3)).performTableWrite(any(Connection.class), any(TableDescriptor.class), groups.capture());
        assertThat(groups.getAllValues().get(0)).containsExactly(encoded1);
        assertThat(groups.getAllValues().get(1)).containsExactly(raw);
        assertThat(groups.getAllValues().get(2)).containsExactly(encoded2);
    }

    @Test
    @FixFor("debezium/dbz#2468")
    void performTableWritesShouldKeepUniformBatchInSingleGroup() throws SQLException {
        final DatabaseDialect dialect = binaryHandlingDialect();
        final DefaultRecordWriter writer = writerSpy(dialect, binaryHandlingEnabledConfig());

        final List<JdbcSinkRecord> records = List.of(bytesRecord(ENCODED_TOPIC), bytesRecord(ENCODED_TOPIC), bytesRecord(ENCODED_TOPIC));
        writer.performTableWrites(mock(Connection.class), table(), records);

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<List<JdbcSinkRecord>> groups = ArgumentCaptor.forClass(List.class);
        verify(writer, times(1)).performTableWrite(any(Connection.class), any(TableDescriptor.class), groups.capture());
        assertThat(groups.getValue()).containsExactlyElementsOf(records);
    }

    @Test
    @FixFor("debezium/dbz#2468")
    void performTableWritesShouldSkipSignatureResolutionWhenBinaryHandlingDisabled() throws SQLException {
        final DatabaseDialect dialect = binaryHandlingDialect();
        final DefaultRecordWriter writer = writerSpy(dialect, getConfig(Map.of(JdbcSinkConnectorConfig.INSERT_MODE, "insert")));

        final List<JdbcSinkRecord> records = List.of(bytesRecord(ENCODED_TOPIC), bytesRecord(RAW_TOPIC));
        writer.performTableWrites(mock(Connection.class), table(), records);

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<List<JdbcSinkRecord>> groups = ArgumentCaptor.forClass(List.class);
        verify(writer, times(1)).performTableWrite(any(Connection.class), any(TableDescriptor.class), groups.capture());
        assertThat(groups.getValue()).containsExactlyElementsOf(records);
        verify(dialect, never()).resolveBinaryHandling(any(), any(), any());
    }

    @Test
    @FixFor("debezium/dbz#2468")
    void performTableWritesShouldSkipSignatureResolutionForSingleRecord() throws SQLException {
        final DatabaseDialect dialect = binaryHandlingDialect();
        final DefaultRecordWriter writer = writerSpy(dialect, binaryHandlingEnabledConfig());

        writer.performTableWrites(mock(Connection.class), table(), List.of(bytesRecord(ENCODED_TOPIC)));

        verify(writer, times(1)).performTableWrite(any(Connection.class), any(TableDescriptor.class), anyList());
        verify(dialect, never()).resolveBinaryHandling(any(), any(), any());
    }

    private DefaultRecordWriter writerSpy(DatabaseDialect dialect, JdbcSinkConnectorConfig config) throws SQLException {
        final DefaultRecordWriter writer = spy(new DefaultRecordWriter(
                mock(SharedSessionContract.class), new QueryBinderResolver(), config, dialect, SinkProgressListener.NO_OP()));
        doNothing().when(writer).performTableWrite(any(Connection.class), any(TableDescriptor.class), anyList());
        return writer;
    }

    /**
     * A dialect whose resolution encodes fields from the encoded topic and keeps raw bytes otherwise,
     * mirroring a topic-qualified selector configuration.
     */
    private DatabaseDialect binaryHandlingDialect() {
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        when(dialect.getSchemaType(Schema.OPTIONAL_BYTES_SCHEMA)).thenReturn(ConnectBytesType.INSTANCE);
        when(dialect.resolveBinaryHandling(any(), any(JdbcSinkRecord.class), any())).thenAnswer(invocation -> {
            final JdbcSinkRecord record = invocation.getArgument(1);
            if (ENCODED_TOPIC.equals(record.topicName())) {
                return new BinaryHandling.Resolution(JdbcSinkConnectorConfig.BinaryHandlingMode.HEX, characterColumn());
            }
            return BinaryHandling.Resolution.bytes(null);
        });
        return dialect;
    }

    private JdbcSinkConnectorConfig binaryHandlingEnabledConfig() {
        return getConfig(Map.of(
                JdbcSinkConnectorConfig.INSERT_MODE, "insert",
                JdbcSinkConnectorConfig.BINARY_HANDLING_MODE, "hex"));
    }

    private static JdbcSinkRecord bytesRecord(String topicName) {
        final JdbcSinkRecord record = mock(JdbcSinkRecord.class);
        when(record.topicName()).thenReturn(topicName);
        when(record.jdbcFields()).thenReturn(Map.of("data",
                new JdbcFieldDescriptor(new FieldDescriptor(Schema.OPTIONAL_BYTES_SCHEMA, "data", false), false)));
        return record;
    }

    private static TableDescriptor table() {
        return TableDescriptor.builder().tableName("t").build();
    }

    private static ColumnDescriptor characterColumn() {
        return ColumnDescriptor.builder()
                .columnName("data")
                .jdbcType(Types.VARCHAR)
                .typeName("text")
                .build();
    }
}
