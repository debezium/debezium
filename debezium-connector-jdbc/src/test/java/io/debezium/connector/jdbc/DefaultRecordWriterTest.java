/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.SharedSessionContract;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.field.JdbcFieldDescriptor;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.field.FieldDescriptor;
import io.debezium.sink.spi.SinkProgressListener;
import io.debezium.time.StructuredTimestamp;

@Tag("UnitTests")
class DefaultRecordWriterTest extends AbstractBaseJdbcSinkTest {

    @Test
    void shouldValidateAllValuesBeforeGeneratingSql() {
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        final var writer = new ExposedRecordWriter(
                mock(SharedSessionContract.class), getConfig(Map.of()), dialect);
        final var timestampSchema = StructuredTimestamp.builder(9).build();
        final var value = StructuredTimestamp.from(timestampSchema, 2026, 7, 17, 12, 13, 14, 123_456_789, 9);
        final var payloadSchema = SchemaBuilder.struct().field("ts", timestampSchema).build();
        final var payload = new Struct(payloadSchema).put("ts", value);
        final var field = new JdbcFieldDescriptor(new FieldDescriptor(timestampSchema, "ts", false), false);
        final var column = ColumnDescriptor.builder()
                .columnName("ts")
                .jdbcType(Types.TIMESTAMP)
                .typeName("timestamp")
                .scale(6)
                .build();
        final var table = TableDescriptor.builder().tableName("target_table").column(column).build();
        final JdbcSinkRecord record = mock(JdbcSinkRecord.class);

        when(record.isDelete()).thenReturn(false);
        when(record.filteredKey()).thenReturn(null);
        when(record.keyFieldNames()).thenReturn(Set.of());
        when(record.nonKeyFieldNames()).thenReturn(Set.of("ts"));
        when(record.getPayload()).thenReturn(payload);
        when(record.jdbcFields()).thenReturn(Map.of("ts", field));
        when(dialect.resolveColumn(table, field)).thenReturn(column);
        doThrow(new ConnectException("precision loss"))
                .when(dialect).validateValue(field, column, value);

        assertThatThrownBy(() -> writer.statement(table, List.of(record)))
                .isInstanceOf(ConnectException.class)
                .hasMessage("precision loss");
        verify(dialect, never()).getInsertStatement(table, record);
    }

    private static class ExposedRecordWriter extends DefaultRecordWriter {

        ExposedRecordWriter(SharedSessionContract session, JdbcSinkConnectorConfig config, DatabaseDialect dialect) {
            super(session, new QueryBinderResolver(), config, dialect, SinkProgressListener.NO_OP());
        }

        void statement(TableDescriptor table, List<JdbcSinkRecord> records) {
            getSqlStatementInfo(table, records);
        }
    }
}
