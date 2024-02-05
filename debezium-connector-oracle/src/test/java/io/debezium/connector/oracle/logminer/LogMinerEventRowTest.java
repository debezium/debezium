/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Calendar;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.doc.FixFor;

/**
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER)
public class LogMinerEventRowTest {

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    private static final String CATALOG_NAME = "DEBEZIUM";
    private ResultSet resultSet;

    @Before
    public void before() {
        resultSet = mock(ResultSet.class);
    }

    @Test
    public void testChangeTime() throws Exception {
        when(resultSet.getTimestamp(eq(4), any(Calendar.class))).thenReturn(new Timestamp(1000L));

        LogMinerEventRow row = LogMinerEventRow.fromResultSet(resultSet, CATALOG_NAME, true);
        assertThat(row.getChangeTime()).isEqualTo(Instant.ofEpochMilli(1000L));

        when(resultSet.getTimestamp(eq(4), any(Calendar.class))).thenThrow(SQLException.class);
        assertThrows(resultSet, SQLException.class);

        verify(resultSet, times(2)).getTimestamp(eq(4), any(Calendar.class));
    }

    @Test
    public void testEventType() throws Exception {
        when(resultSet.getInt(3)).thenReturn(EventType.UPDATE.getValue());

        LogMinerEventRow row = LogMinerEventRow.fromResultSet(resultSet, CATALOG_NAME, true);
        assertThat(row.getEventType()).isEqualTo(EventType.UPDATE);
        verify(resultSet).getInt(3);

        when(resultSet.getInt(3)).thenThrow(SQLException.class);
        assertThrows(resultSet, SQLException.class);

        verify(resultSet, times(2)).getInt(3);
    }

    @Test
    public void testTableName() throws Exception {
        when(resultSet.getString(7)).thenReturn("TABLENAME");

        LogMinerEventRow row = LogMinerEventRow.fromResultSet(resultSet, CATALOG_NAME, true);
        assertThat(row.getTableName()).isEqualTo("TABLENAME");
        verify(resultSet).getString(7);

        when(resultSet.getString(7)).thenThrow(SQLException.class);
        assertThrows(resultSet, SQLException.class);

        verify(resultSet, times(2)).getString(7);
    }

    @Test
    public void testTablespaceName() throws Exception {
        when(resultSet.getString(8)).thenReturn("DEBEZIUM");

        LogMinerEventRow row = LogMinerEventRow.fromResultSet(resultSet, CATALOG_NAME, true);
        assertThat(row.getTablespaceName()).isEqualTo("DEBEZIUM");
        verify(resultSet).getString(8);

        when(resultSet.getString(8)).thenThrow(SQLException.class);
        assertThrows(resultSet, SQLException.class);

        verify(resultSet, times(2)).getString(8);
    }

    @Test
    public void testScn() throws Exception {
        when(resultSet.getString(1)).thenReturn("12345");

        LogMinerEventRow row = LogMinerEventRow.fromResultSet(resultSet, CATALOG_NAME, true);
        assertThat(row.getScn()).isEqualTo(Scn.valueOf(12345L));
        verify(resultSet).getString(1);

        when(resultSet.getString(1)).thenThrow(SQLException.class);
        assertThrows(resultSet, SQLException.class);

        verify(resultSet, times(2)).getString(1);
    }

    @Test
    public void testTransactionId() throws Exception {
        when(resultSet.getBytes(5)).thenReturn("tr_id".getBytes(StandardCharsets.UTF_8));

        LogMinerEventRow row = LogMinerEventRow.fromResultSet(resultSet, CATALOG_NAME, true);
        assertThat(row.getTransactionId()).isEqualToIgnoringCase("74725F6964");
        verify(resultSet).getBytes(5);

        when(resultSet.getBytes(5)).thenThrow(SQLException.class);
        assertThrows(resultSet, SQLException.class);

        verify(resultSet, times(2)).getBytes(5);
    }

    @Test
    public void testTableId() throws Exception {
        when(resultSet.getString(8)).thenReturn("SCHEMA");
        when(resultSet.getString(7)).thenReturn("TABLE");

        LogMinerEventRow row = LogMinerEventRow.fromResultSet(resultSet, CATALOG_NAME, true);
        assertThat(row.getTableId().toString()).isEqualTo("DEBEZIUM.SCHEMA.TABLE");
        verify(resultSet).getString(8);

        when(resultSet.getString(8)).thenThrow(SQLException.class);
        assertThrows(resultSet, SQLException.class);
    }

    @Test
    @FixFor("DBZ-2555")
    public void tesetTableIdWithVariedCase() throws Exception {
        when(resultSet.getString(8)).thenReturn("Schema");
        when(resultSet.getString(7)).thenReturn("table");

        LogMinerEventRow row = LogMinerEventRow.fromResultSet(resultSet, CATALOG_NAME, true);
        assertThat(row.getTableId().toString()).isEqualTo("DEBEZIUM.Schema.table");
        verify(resultSet).getString(8);

        when(resultSet.getString(8)).thenThrow(SQLException.class);
        assertThrows(resultSet, SQLException.class);
    }

    @Test
    public void testSqlRedo() throws Exception {
        when(resultSet.getInt(6)).thenReturn(0);
        when(resultSet.getString(2)).thenReturn("short_sql");

        LogMinerEventRow row = LogMinerEventRow.fromResultSet(resultSet, CATALOG_NAME, true);
        assertThat(row.getRedoSql()).isEqualTo("short_sql");
        verify(resultSet).getInt(6);
        verify(resultSet).getString(2);

        when(resultSet.getInt(6)).thenReturn(1).thenReturn(0);
        when(resultSet.getString(2)).thenReturn("long").thenReturn("_sql");

        row = LogMinerEventRow.fromResultSet(resultSet, CATALOG_NAME, true);
        assertThat(row.getRedoSql()).isEqualTo("long_sql");
        verify(resultSet, times(3)).getInt(6);
        verify(resultSet, times(3)).getString(2);

        // Test very large SQL
        char[] chars = new char[4000];
        Arrays.fill(chars, 'a');
        when(resultSet.getString(2)).thenReturn(new String(chars));
        when(resultSet.getInt(6)).thenReturn(1, 1, 1, 1, 1, 1, 1, 1, 1, 0);

        row = LogMinerEventRow.fromResultSet(resultSet, CATALOG_NAME, true);
        assertThat(row.getRedoSql().length()).isEqualTo(40_000);
        verify(resultSet, times(13)).getInt(6);
        verify(resultSet, times(13)).getString(2);

        when(resultSet.getInt(6)).thenReturn(0);
        when(resultSet.getString(2)).thenReturn(null);

        row = LogMinerEventRow.fromResultSet(resultSet, CATALOG_NAME, true);
        assertThat(row.getRedoSql()).isNull();
        verify(resultSet, times(14)).getInt(6);
        verify(resultSet, times(14)).getString(2);

        when(resultSet.getInt(6)).thenReturn(0);
        when(resultSet.getString(2)).thenThrow(SQLException.class);

        assertThrows(resultSet, SQLException.class);

        verify(resultSet, times(15)).getInt(6);
        verify(resultSet, times(15)).getString(2);
    }

    private static <T extends Throwable, R> void assertThrows(ResultSet rs, Class<T> throwAs) {
        try {
            LogMinerEventRow.fromResultSet(rs, CATALOG_NAME, true);
            fail("Should have thrown a " + throwAs.getSimpleName());
        }
        catch (Throwable t) {
            assertThat(t).isInstanceOf(throwAs);
        }
    }
}
