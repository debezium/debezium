/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.SharedSessionContract;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.field.JdbcFieldDescriptor;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.spi.SinkProgressListener;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

@Tag("UnitTests")
class UnnestRecordWriterTest extends AbstractBaseJdbcSinkTest {

    @Test
    void performTableWriteShouldDispatchToUnnestWhenBatchStatement() throws SQLException {
        SharedSessionContract session = mock(SharedSessionContract.class);
        DatabaseDialect dialect = mock(DatabaseDialect.class);
        JdbcSinkConnectorConfig config = getConfig(Map.of(
                JdbcSinkConnectorConfig.INSERT_MODE, "insert",
                JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, "true"));

        UnnestRecordWriter writer = spy(new UnnestRecordWriter(
                session, new QueryBinderResolver(), config, dialect, SinkProgressListener.NO_OP()));

        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        TableDescriptor table = TableDescriptor.builder().tableName("test_table").build();
        JdbcSinkRecord record = mock(JdbcSinkRecord.class);

        when(record.isDelete()).thenReturn(false);
        when(record.keyFieldNames()).thenReturn(Set.of());
        when(record.nonKeyFieldNames()).thenReturn(Set.of());

        String unnestSql = "INSERT INTO test_table SELECT * FROM UNNEST(?::int[])";
        doReturn(new RecordWriter.SqlStatementInfo(unnestSql, true, false))
                .when(writer).getSqlStatementInfo(any(TableDescriptor.class), any());

        when(conn.prepareStatement(unnestSql)).thenReturn(ps);
        when(ps.executeUpdate()).thenReturn(0);

        writer.performTableWrite(conn, table, List.of(record));

        verify(conn).prepareStatement(unnestSql);
        verify(ps).executeUpdate();
    }

    @Test
    void performTableWriteShouldFallBackToStandardWhenNotBatchStatement() throws SQLException {
        SharedSessionContract session = mock(SharedSessionContract.class);
        DatabaseDialect dialect = mock(DatabaseDialect.class);
        JdbcSinkConnectorConfig config = getConfig(Map.of(
                JdbcSinkConnectorConfig.INSERT_MODE, "insert",
                JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, "true"));

        UnnestRecordWriter writer = spy(new UnnestRecordWriter(
                session, new QueryBinderResolver(), config, dialect, SinkProgressListener.NO_OP()));

        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        TableDescriptor table = TableDescriptor.builder().tableName("test_table").build();
        JdbcSinkRecord record = mock(JdbcSinkRecord.class);

        when(record.isDelete()).thenReturn(false);
        when(record.keyFieldNames()).thenReturn(Set.of());
        when(record.nonKeyFieldNames()).thenReturn(Set.of());

        String standardSql = "INSERT INTO test_table (col) VALUES (?)";
        doReturn(new RecordWriter.SqlStatementInfo(standardSql, false, false))
                .when(writer).getSqlStatementInfo(any(TableDescriptor.class), any());

        when(conn.prepareStatement(standardSql)).thenReturn(ps);
        when(ps.executeBatch()).thenReturn(new int[0]);

        writer.performTableWrite(conn, table, List.of(record));

        verify(conn).prepareStatement(standardSql);
        verify(ps).executeBatch();
    }

    @Test
    void performUnnestBatchShouldTransformValuesViaDatabaseDialect() throws SQLException {
        SharedSessionContract session = mock(SharedSessionContract.class);
        DatabaseDialect dialect = mock(DatabaseDialect.class);
        JdbcSinkConnectorConfig config = getConfig(Map.of(
                JdbcSinkConnectorConfig.INSERT_MODE, "insert",
                JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, "true"));

        UnnestRecordWriter writer = new UnnestRecordWriter(
                session, new QueryBinderResolver(), config, dialect, SinkProgressListener.NO_OP());

        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        Array sqlArray = mock(Array.class);
        when(conn.prepareStatement(any())).thenReturn(ps);
        when(conn.createArrayOf(any(), any())).thenReturn(sqlArray);
        when(ps.executeUpdate()).thenReturn(1);

        Schema timestampSchema = io.debezium.time.Timestamp.schema();
        Schema dateSchema = io.debezium.time.Date.schema();

        JdbcFieldDescriptor tsField = new JdbcFieldDescriptor(
                new io.debezium.sink.field.FieldDescriptor(timestampSchema, "ts_col", false), false);
        JdbcFieldDescriptor dateField = new JdbcFieldDescriptor(
                new io.debezium.sink.field.FieldDescriptor(dateSchema, "date_col", false), false);

        JdbcType tsType = mock(JdbcType.class);
        JdbcType dateType = mock(JdbcType.class);
        when(tsType.getTypeName(any(), any(Boolean.class))).thenReturn("timestamp");
        when(dateType.getTypeName(any(), any(Boolean.class))).thenReturn("date");

        when(dialect.getSchemaType(timestampSchema)).thenReturn(tsType);
        when(dialect.getSchemaType(dateSchema)).thenReturn(dateType);

        LocalDateTime transformedTs = LocalDateTime.of(2024, 1, 15, 10, 30, 0);
        when(dialect.bindValue(any(JdbcFieldDescriptor.class), anyInt(), any()))
                .thenAnswer(invocation -> {
                    JdbcFieldDescriptor field = invocation.getArgument(0);
                    Object value = invocation.getArgument(2);
                    if (field.getSchema().name().equals(io.debezium.time.Timestamp.SCHEMA_NAME)) {
                        return List.of(new ValueBindDescriptor(1, transformedTs));
                    }
                    else if (field.getSchema().name().equals(io.debezium.time.Date.SCHEMA_NAME)) {
                        return List.of(new ValueBindDescriptor(1, LocalDate.of(2024, 6, 20)));
                    }
                    return List.of(new ValueBindDescriptor(1, value));
                });

        Set<String> nonKeyFields = new LinkedHashSet<>();
        nonKeyFields.add("ts_col");
        nonKeyFields.add("date_col");

        Struct payload = mock(Struct.class);
        when(payload.get("ts_col")).thenReturn(1705312200000L);
        when(payload.get("date_col")).thenReturn(19894);

        JdbcSinkRecord record = mock(JdbcSinkRecord.class);
        when(record.isDelete()).thenReturn(false);
        when(record.keyFieldNames()).thenReturn(Set.of());
        when(record.nonKeyFieldNames()).thenReturn(nonKeyFields);
        when(record.getPayload()).thenReturn(payload);
        when(record.jdbcFields()).thenReturn(Map.of("ts_col", tsField, "date_col", dateField));

        writer.performUnnestBatch(conn, "INSERT INTO t SELECT * FROM UNNEST(?::timestamp[],?::date[])", List.of(record));

        // Verify dialect.bindValue was called for value transformation
        verify(dialect).bindValue(tsField, 1, 1705312200000L);
        verify(dialect).bindValue(dateField, 1, 19894);

        // Verify arrays were created and bound
        verify(conn).createArrayOf("timestamp", new Object[]{ transformedTs });
        verify(conn).createArrayOf("date", new Object[]{ LocalDate.of(2024, 6, 20) });
    }

    @Test
    void performUnnestBatchShouldRejectMultiBindTypes() throws SQLException {
        SharedSessionContract session = mock(SharedSessionContract.class);
        DatabaseDialect dialect = mock(DatabaseDialect.class);
        JdbcSinkConnectorConfig config = getConfig(Map.of(
                JdbcSinkConnectorConfig.INSERT_MODE, "insert",
                JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, "true"));

        UnnestRecordWriter writer = new UnnestRecordWriter(
                session, new QueryBinderResolver(), config, dialect, SinkProgressListener.NO_OP());

        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(ps);

        Schema geometrySchema = mock(Schema.class);
        when(geometrySchema.isOptional()).thenReturn(true);
        when(geometrySchema.name()).thenReturn("io.debezium.data.geometry.Geometry");

        JdbcFieldDescriptor geoField = new JdbcFieldDescriptor(
                new io.debezium.sink.field.FieldDescriptor(geometrySchema, "geom", false), false);

        JdbcType geoType = mock(JdbcType.class);
        when(geoType.getTypeName(any(), any(Boolean.class))).thenReturn("geometry");
        when(dialect.getSchemaType(geometrySchema)).thenReturn(geoType);

        // Geometry returns TWO ValueBindDescriptors (wkb bytes + srid)
        when(dialect.bindValue(any(JdbcFieldDescriptor.class), anyInt(), any()))
                .thenReturn(List.of(
                        new ValueBindDescriptor(1, new byte[]{ 0x01 }),
                        new ValueBindDescriptor(2, 4326)));

        Set<String> nonKeyFields = new LinkedHashSet<>();
        nonKeyFields.add("geom");

        Struct payload = mock(Struct.class);
        when(payload.getWithoutDefault("geom")).thenReturn(mock(Struct.class));

        JdbcSinkRecord record = mock(JdbcSinkRecord.class);
        when(record.isDelete()).thenReturn(false);
        when(record.keyFieldNames()).thenReturn(Set.of());
        when(record.nonKeyFieldNames()).thenReturn(nonKeyFields);
        when(record.getPayload()).thenReturn(payload);
        when(record.jdbcFields()).thenReturn(Map.of("geom", geoField));

        assertThatThrownBy(() -> writer.performUnnestBatch(conn, "INSERT INTO t SELECT * FROM UNNEST(?::geometry[])", List.of(record)))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("UNNEST does not support types that expand to multiple bind parameters")
                .hasMessageContaining("geom");
    }
}
