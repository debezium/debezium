/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.sql.SQLRecoverableException;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName;
import io.debezium.relational.TableId;

@SkipWhenAdapterNameIsNot(value = AdapterName.LOGMINER)
public class SqlUtilsTest {

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    @Test
    public void testStatements() {
        SqlUtils.setRac(false);

        String result = SqlUtils.addLogFileStatement("ADD", "FILENAME");
        String expected = "BEGIN sys.dbms_logmnr.add_logfile(LOGFILENAME => 'FILENAME', OPTIONS => ADD);END;";
        assertThat(expected.equals(result)).isTrue();

        OracleDatabaseSchema schema = mock(OracleDatabaseSchema.class);
        TableId table1 = new TableId("catalog", "schema", "table1");
        TableId table2 = new TableId("catalog", "schema", "table2");
        Set<TableId> tables = new HashSet<>();
        Mockito.when(schema.tableIds()).thenReturn(tables);
        result = SqlUtils.logMinerContentsQuery("DATABASE", "SCHEMA", schema);
        expected = "SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME  " +
                "FROM V$LOGMNR_CONTENTS WHERE  OPERATION_CODE in (1,2,3,5)  AND SEG_OWNER = 'DATABASE'  AND SCN >= ? AND SCN < ?  " +
                "OR (OPERATION_CODE IN (5,7,34,36) AND USERNAME NOT IN ('SYS','SYSTEM','SCHEMA'))ORDER BY SCN";
        assertThat(expected.equals(result)).isTrue();

        tables.add(table1);
        tables.add(table2);
        result = SqlUtils.logMinerContentsQuery("DATABASE", "SCHEMA", schema);
        expected = "SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME  " +
                "FROM V$LOGMNR_CONTENTS WHERE  OPERATION_CODE in (1,2,3,5)  " +
                "AND SEG_OWNER = 'DATABASE'  AND TABLE_NAME IN ('table1','table2') AND SEG_NAME IN ('table1','table2')  " +
                "AND SCN >= ? AND SCN < ?  OR (OPERATION_CODE IN (5,7,34,36) AND USERNAME NOT IN ('SYS','SYSTEM','SCHEMA'))ORDER BY SCN";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.startLogMinerStatement(10L, 20L, OracleConnectorConfig.LogMiningStrategy.ONLINE_CATALOG, true);
        expected = "BEGIN sys.dbms_logmnr.start_logmnr(startScn => '10', endScn => '20', " +
                "OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG  + DBMS_LOGMNR.CONTINUOUS_MINE  + DBMS_LOGMNR.NO_ROWID_IN_STMT);END;";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.startLogMinerStatement(10L, 20L, OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO, false);
        expected = "BEGIN sys.dbms_logmnr.start_logmnr(startScn => '10', endScn => '20', " +
                "OPTIONS => DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING  + DBMS_LOGMNR.NO_ROWID_IN_STMT);END;";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.truncateTableStatement("table_name");
        expected = "TRUNCATE TABLE table_name";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.diffInDaysQuery(123L);
        expected = "select sysdate - CAST(scn_to_timestamp(123) as date) from dual";
        assertThat(expected.equals(result)).isTrue();
        result = SqlUtils.diffInDaysQuery(null);
        assertThat(result).isNull();

        result = SqlUtils.bulkHistoryInsertStmt("table_name");
        expected = "INSERT  /*+ APPEND */ INTO table_name SELECT * FROM LOG_MINING_TEMP";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.redoLogStatusQuery();
        expected = "SELECT F.MEMBER, R.STATUS FROM V$LOGFILE F, V$LOG R WHERE F.GROUP# = R.GROUP# ORDER BY 2";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.switchHistoryQuery();
        expected = "SELECT 'TOTAL', COUNT(1) FROM V$ARCHIVED_LOG WHERE FIRST_TIME > TRUNC(SYSDATE)" +
                " AND DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL')";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.currentRedoNameQuery();
        expected = "SELECT F.MEMBER FROM V$LOG LOG, V$LOGFILE F  WHERE LOG.GROUP#=F.GROUP# AND LOG.STATUS='CURRENT'";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.supplementalLoggingCheckQuery();
        expected = "SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.currentScnQuery();
        expected = "SELECT CURRENT_SCN FROM V$DATABASE";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.oldestFirstChangeQuery();
        expected = "SELECT MIN(FIRST_CHANGE#) FROM V$LOG";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.allOnlineLogsQuery();
        expected = "SELECT MIN(F.MEMBER) AS FILE_NAME, L.NEXT_CHANGE# AS NEXT_CHANGE, F.GROUP# " +
                " FROM V$LOG L, V$LOGFILE F " +
                " WHERE F.GROUP# = L.GROUP# AND L.NEXT_CHANGE# > 0 " +
                " GROUP BY F.GROUP#, L.NEXT_CHANGE# ORDER BY 3";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.tableExistsQuery("table_name");
        expected = "SELECT '1' AS ONE FROM USER_TABLES WHERE TABLE_NAME = 'table_name'";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.logMiningHistoryDdl("table_name");
        expected = "create  TABLE table_name(" +
                "row_sequence NUMBER(19,0), " +
                "captured_scn NUMBER(19,0), " +
                "table_name VARCHAR2(30 CHAR), " +
                "seg_owner VARCHAR2(30 CHAR), " +
                "operation_code NUMBER(19,0), " +
                "change_time TIMESTAMP(6), " +
                "transaction_id VARCHAR2(50 CHAR), " +
                "csf NUMBER(19,0), " +
                "redo_sql VARCHAR2(4000 CHAR)" +
                ") nologging";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.oneDayArchivedLogsQuery(10L);
        expected = "SELECT NAME AS FILE_NAME, NEXT_CHANGE# AS NEXT_CHANGE FROM V$ARCHIVED_LOG " +
                " WHERE NAME IS NOT NULL AND FIRST_TIME >= SYSDATE - 1 AND ARCHIVED = 'YES' " +
                " AND STATUS = 'A' AND NEXT_CHANGE# > 10 ORDER BY 2";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.deleteLogFileStatement("file_name");
        expected = "BEGIN SYS.DBMS_LOGMNR.REMOVE_LOGFILE(LOGFILENAME => 'file_name');END;";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.getHistoryTableNamesQuery();
        expected = "SELECT TABLE_NAME, '1' FROM USER_TABLES WHERE TABLE_NAME LIKE 'LM_HIST_%'";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.dropHistoryTableStatement("table_name");
        expected = "DROP TABLE TABLE_NAME PURGE";
        assertThat(expected.equals(result)).isTrue();

    }

    @Test
    public void shouldParseHistoryTableNames() {
        String name = SqlUtils.buildHistoryTableName(LocalDateTime.now());
        long diff = SqlUtils.parseRetentionFromName(name);
        assertThat(diff).isEqualTo(0);

        name = SqlUtils.buildHistoryTableName(LocalDateTime.now().minusHours(10));
        diff = SqlUtils.parseRetentionFromName(name);
        assertThat(diff).isEqualTo(10);

        diff = SqlUtils.parseRetentionFromName(SqlUtils.LOGMNR_HISTORY_TABLE_PREFIX + "10_2_4_5");
        assertThat(diff).isEqualTo(0);

    }

    @Test
    public void shouldDetectConnectionProblems() {
        assertThat(SqlUtils.connectionProblem(new IOException("connection"))).isTrue();
        assertThat(SqlUtils.connectionProblem(new SQLRecoverableException("connection"))).isTrue();
        assertThat(SqlUtils.connectionProblem(new Throwable())).isFalse();
        assertThat(SqlUtils.connectionProblem(new Exception("ORA-03135 problem"))).isTrue();
        assertThat(SqlUtils.connectionProblem(new Exception("ORA-12543 problem"))).isTrue();
        assertThat(SqlUtils.connectionProblem(new Exception("ORA-00604 problem"))).isTrue();
        assertThat(SqlUtils.connectionProblem(new Exception("ORA-01089 problem"))).isTrue();
        assertThat(SqlUtils.connectionProblem(new Exception("ORA-00600 problem"))).isTrue();
        assertThat(SqlUtils.connectionProblem(new Exception("ORA-99999 problem"))).isFalse();
        assertThat(SqlUtils.connectionProblem(new Exception("NO MORE DATA TO READ FROM SOCKET problem"))).isTrue();

        assertThat(SqlUtils.connectionProblem(new Exception("12543 problem"))).isFalse();
    }
}
