/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName;
import io.debezium.doc.FixFor;
import io.debezium.relational.TableId;

@SkipWhenAdapterNameIsNot(value = AdapterName.LOGMINER)
public class RowMapperTest {

    private static final Scn SCN_ONE = new Scn(BigInteger.ONE);

    private ResultSet rs;

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    @Before
    public void before() {
        rs = mock(ResultSet.class);
    }

    @Test
    public void testChangeTime() throws SQLException {
        Mockito.when(rs.getTimestamp(eq(4), any(Calendar.class))).thenReturn(new Timestamp(1000L));
        Timestamp time = RowMapper.getChangeTime(rs);
        assertThat(time.getTime()).isEqualTo(1000L);
        Mockito.when(rs.getTimestamp(eq(4), any(Calendar.class))).thenThrow(SQLException.class);
        try {
            time = RowMapper.getChangeTime(rs);
            fail("Should have thrown a SQLException");
        }
        catch (SQLException e) {
            // expected
        }
        verify(rs, times(2)).getTimestamp(eq(4), any(Calendar.class));
    }

    @Test
    public void testOperationCode() throws SQLException {
        Mockito.when(rs.getInt(3)).thenReturn(100);
        int operation = RowMapper.getOperationCode(rs);
        assertThat(operation).isEqualTo(100);
        verify(rs).getInt(3);
        Mockito.when(rs.getInt(3)).thenThrow(SQLException.class);
        try {
            operation = RowMapper.getOperationCode(rs);
            fail("Should have thrown a SQLException");
        }
        catch (SQLException e) {
            // expected
        }
        verify(rs, times(2)).getInt(3);
    }

    @Test
    public void testTableName() throws SQLException {
        Mockito.when(rs.getString(7)).thenReturn("table_name");
        String tableName = RowMapper.getTableName(rs);
        assertThat(tableName.equals("table_name")).isTrue();
        verify(rs).getString(7);
        Mockito.when(rs.getString(7)).thenThrow(SQLException.class);
        try {
            tableName = RowMapper.getTableName(rs);
            fail("Should have thrown a SQLException");
        }
        catch (SQLException e) {
            // expected
        }
        verify(rs, times(2)).getString(7);
    }

    @Test
    public void testSeqOwner() throws SQLException {
        Mockito.when(rs.getString(8)).thenReturn("owner");
        String owner = RowMapper.getSegOwner(rs);
        assertThat(owner.equals("owner")).isTrue();
        verify(rs).getString(8);
        Mockito.when(rs.getString(8)).thenThrow(SQLException.class);
        try {
            owner = RowMapper.getSegOwner(rs);
            fail("Should have thrown a SQLException");
        }
        catch (SQLException e) {
            // expected
        }
        verify(rs, times(2)).getString(8);
    }

    @Test
    public void testGetScn() throws SQLException {
        Mockito.when(rs.getString(1)).thenReturn("1");
        Scn scn = RowMapper.getScn(rs);
        assertThat(scn).isEqualTo(Scn.valueOf(1L));
        verify(rs).getString(1);
        Mockito.when(rs.getString(1)).thenThrow(SQLException.class);
        try {
            scn = RowMapper.getScn(rs);
            fail("Should have thrown a SQLException");
        }
        catch (SQLException e) {
            // expected
        }
        verify(rs, times(2)).getString(1);
    }

    @Test
    public void testGetTransactionId() throws SQLException {
        Mockito.when(rs.getBytes(5)).thenReturn("tr_id".getBytes());
        String transactionId = RowMapper.getTransactionId(rs);
        assertThat(transactionId).isEqualToIgnoringCase("74725F6964");
        verify(rs).getBytes(5);
        Mockito.when(rs.getBytes(5)).thenThrow(SQLException.class);
        try {
            transactionId = RowMapper.getTransactionId(rs);
            fail("Should have thrown a SQLException");
        }
        catch (SQLException e) {
            // expected
        }
        verify(rs, times(2)).getBytes(5);
    }

    @Test
    public void testSqlRedo() throws SQLException {
        Mockito.when(rs.getInt(6)).thenReturn(0);
        Mockito.when(rs.getString(2)).thenReturn("short_sql");
        String sql = RowMapper.getSqlRedo(rs, false, null, SCN_ONE, "", "", 1, null, "");
        assertThat(sql.equals("short_sql")).isTrue();
        verify(rs).getInt(6);
        verify(rs).getString(2);

        Mockito.when(rs.getInt(6)).thenReturn(1).thenReturn(0);
        Mockito.when(rs.getString(2)).thenReturn("long").thenReturn("_sql");
        sql = RowMapper.getSqlRedo(rs, false, null, SCN_ONE, "", "", 1, null, "");
        assertThat(sql.equals("long_sql")).isTrue();
        verify(rs, times(3)).getInt(6);
        verify(rs, times(3)).getString(2);

        // test super large DML
        char[] chars = new char[4000];
        Arrays.fill(chars, 'a');
        Mockito.when(rs.getString(2)).thenReturn(new String(chars));
        Mockito.when(rs.getInt(6)).thenReturn(1);
        sql = RowMapper.getSqlRedo(rs, false, null, SCN_ONE, "", "", 1, null, "");
        assertThat(sql.length()).isEqualTo(40_000);
        verify(rs, times(13)).getInt(6);
        verify(rs, times(13)).getString(2);

        Mockito.when(rs.getInt(6)).thenReturn(0);
        Mockito.when(rs.getString(2)).thenReturn(null);
        sql = RowMapper.getSqlRedo(rs, false, null, SCN_ONE, "", "", 1, null, "");
        assertThat(sql).isNull();
        verify(rs, times(13)).getInt(6);
        verify(rs, times(14)).getString(2);

        Mockito.when(rs.getInt(6)).thenReturn(0);
        Mockito.when(rs.getString(2)).thenThrow(SQLException.class);
        try {
            sql = RowMapper.getSqlRedo(rs, false, null, SCN_ONE, "", "", 1, null, "");
            fail("Should have thrown a SQLException");
        }
        catch (SQLException e) {
            // expected
        }
        verify(rs, times(13)).getInt(6);
        verify(rs, times(15)).getString(2);

    }

    @Test
    public void testGetTableId() throws SQLException {
        Mockito.when(rs.getString(8)).thenReturn("SCHEMA");
        Mockito.when(rs.getString(7)).thenReturn("TABLE");
        TableId tableId = RowMapper.getTableId("CATALOG", rs);
        assertThat(tableId.toString().equals("CATALOG.SCHEMA.TABLE")).isTrue();
        verify(rs).getString(8);
        Mockito.when(rs.getString(8)).thenThrow(SQLException.class);

        tableId = null;
        try {
            tableId = RowMapper.getTableId("catalog", rs);
            fail("Should have thrown a SQLException");
        }
        catch (SQLException e) {
            // expected
            assertThat(tableId).isNull();
        }
    }

    @Test
    @FixFor("DBZ-2555")
    public void testGetTableIdWithVariedCase() throws SQLException {
        Mockito.when(rs.getString(8)).thenReturn("Schema");
        Mockito.when(rs.getString(7)).thenReturn("table");
        TableId tableId = RowMapper.getTableId("CATALOG", rs);
        assertThat(tableId.toString().equals("CATALOG.Schema.table")).isTrue();
        verify(rs).getString(8);
        Mockito.when(rs.getString(8)).thenThrow(SQLException.class);

        tableId = null;
        try {
            tableId = RowMapper.getTableId("catalog", rs);
            fail("Should have thrown a SQLException");
        }
        catch (SQLException e) {
            // expected
            assertThat(tableId).isNull();
        }
    }
}
