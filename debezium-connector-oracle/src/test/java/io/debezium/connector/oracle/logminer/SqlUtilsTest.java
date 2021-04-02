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
import java.time.Duration;
import java.util.Iterator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName;
import io.debezium.doc.FixFor;
import io.debezium.relational.TableId;

@SkipWhenAdapterNameIsNot(value = AdapterName.LOGMINER)
public class SqlUtilsTest {

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    private static final String LOG_MINER_CONTENT_QUERY_TEMPLATE = "SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, " +
            "XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME " +
            "FROM V$LOGMNR_CONTENTS WHERE SCN > ? AND SCN <= ? AND ((" +
            "OPERATION_CODE IN (5,34) AND USERNAME NOT IN ('SYS','SYSTEM','${user}')) " +
            "OR (OPERATION_CODE IN (7,36)) " +
            "OR (OPERATION_CODE IN (1,2,3) " +
            "AND TABLE_NAME != '" + SqlUtils.LOGMNR_FLUSH_TABLE + "' " +
            "${systemTablePredicate}" +
            "${schemaPredicate}" +
            "${tablePredicate}" +
            "))";

    private static final String USERNAME = "USERNAME";

    @Test
    @FixFor("DBZ-3009")
    public void testLogMinerQueryWithNoFilters() {
        OracleConnectorConfig config = mock(OracleConnectorConfig.class);
        Mockito.when(config.schemaIncludeList()).thenReturn(null);
        Mockito.when(config.schemaExcludeList()).thenReturn(null);
        Mockito.when(config.tableIncludeList()).thenReturn(null);
        Mockito.when(config.tableExcludeList()).thenReturn(null);

        String result = SqlUtils.logMinerContentsQuery(config, USERNAME);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(null, null));
    }

    @Test
    @FixFor("DBZ-3009")
    public void testLogMinerQueryWithSchemaInclude() {
        OracleConnectorConfig config = mock(OracleConnectorConfig.class);
        Mockito.when(config.schemaIncludeList()).thenReturn("SCHEMA1,SCHEMA2");
        Mockito.when(config.schemaExcludeList()).thenReturn(null);
        Mockito.when(config.tableIncludeList()).thenReturn(null);
        Mockito.when(config.tableExcludeList()).thenReturn(null);

        String schema = "AND (REGEXP_LIKE(SEG_OWNER,'^SCHEMA1$','i') OR REGEXP_LIKE(SEG_OWNER,'^SCHEMA2$','i')) ";

        String result = SqlUtils.logMinerContentsQuery(config, USERNAME);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(schema, null));
    }

    @Test
    @FixFor("DBZ-3009")
    public void testLogMinerQueryWithSchemaExclude() {
        OracleConnectorConfig config = mock(OracleConnectorConfig.class);
        Mockito.when(config.schemaIncludeList()).thenReturn(null);
        Mockito.when(config.schemaExcludeList()).thenReturn("SCHEMA1,SCHEMA2");
        Mockito.when(config.tableIncludeList()).thenReturn(null);
        Mockito.when(config.tableExcludeList()).thenReturn(null);

        String schema = "AND (NOT REGEXP_LIKE(SEG_OWNER,'^SCHEMA1$','i') AND NOT REGEXP_LIKE(SEG_OWNER,'^SCHEMA2$','i')) ";

        String result = SqlUtils.logMinerContentsQuery(config, USERNAME);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(schema, null));
    }

    @Test
    @FixFor("DBZ-3009")
    public void testLogMinerQueryWithTableInclude() {
        OracleConnectorConfig config = mock(OracleConnectorConfig.class);
        Mockito.when(config.schemaIncludeList()).thenReturn(null);
        Mockito.when(config.schemaExcludeList()).thenReturn(null);
        Mockito.when(config.tableIncludeList()).thenReturn("DEBEZIUM\\.TABLEA,DEBEZIUM\\.TABLEB");
        Mockito.when(config.tableExcludeList()).thenReturn(null);

        String table = "AND (REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEA$','i') " +
                "OR REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEB$','i')) ";

        String result = SqlUtils.logMinerContentsQuery(config, USERNAME);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(null, table));
    }

    @Test
    @FixFor("DBZ-3009")
    public void testLogMinerQueryWithTableExcludes() {
        OracleConnectorConfig config = mock(OracleConnectorConfig.class);
        Mockito.when(config.schemaIncludeList()).thenReturn(null);
        Mockito.when(config.schemaExcludeList()).thenReturn(null);
        Mockito.when(config.tableIncludeList()).thenReturn(null);
        Mockito.when(config.tableExcludeList()).thenReturn("DEBEZIUM\\.TABLEA,DEBEZIUM\\.TABLEB");

        String table = "AND (NOT REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEA$','i') " +
                "AND NOT REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEB$','i')) ";

        String result = SqlUtils.logMinerContentsQuery(config, USERNAME);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(null, table));
    }

    @Test
    @FixFor("DBZ-3009")
    public void testLogMinerQueryWithSchemaTableIncludes() {
        OracleConnectorConfig config = mock(OracleConnectorConfig.class);
        Mockito.when(config.schemaIncludeList()).thenReturn("SCHEMA1,SCHEMA2");
        Mockito.when(config.schemaExcludeList()).thenReturn(null);
        Mockito.when(config.tableIncludeList()).thenReturn("DEBEZIUM\\.TABLEA,DEBEZIUM\\.TABLEB");
        Mockito.when(config.tableExcludeList()).thenReturn(null);

        String schema = "AND (REGEXP_LIKE(SEG_OWNER,'^SCHEMA1$','i') OR REGEXP_LIKE(SEG_OWNER,'^SCHEMA2$','i')) ";
        String table = "AND (REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEA$','i') " +
                "OR REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEB$','i')) ";

        String result = SqlUtils.logMinerContentsQuery(config, USERNAME);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(schema, table));
    }

    @Test
    @FixFor("DBZ-3009")
    public void testLogMinerQueryWithSchemaTableExcludes() {
        OracleConnectorConfig config = mock(OracleConnectorConfig.class);
        Mockito.when(config.schemaIncludeList()).thenReturn(null);
        Mockito.when(config.schemaExcludeList()).thenReturn("SCHEMA1,SCHEMA2");
        Mockito.when(config.tableIncludeList()).thenReturn(null);
        Mockito.when(config.tableExcludeList()).thenReturn("DEBEZIUM\\.TABLEA,DEBEZIUM\\.TABLEB");

        String schema = "AND (NOT REGEXP_LIKE(SEG_OWNER,'^SCHEMA1$','i') AND NOT REGEXP_LIKE(SEG_OWNER,'^SCHEMA2$','i')) ";
        String table = "AND (NOT REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEA$','i') " +
                "AND NOT REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEB$','i')) ";

        String result = SqlUtils.logMinerContentsQuery(config, USERNAME);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(schema, table));
    }

    @Test
    @FixFor("DBZ-3009")
    public void testLogMinerQueryWithSchemaExcludeTableInclude() {
        OracleConnectorConfig config = mock(OracleConnectorConfig.class);
        Mockito.when(config.schemaIncludeList()).thenReturn(null);
        Mockito.when(config.schemaExcludeList()).thenReturn("SCHEMA1,SCHEMA2");
        Mockito.when(config.tableIncludeList()).thenReturn("DEBEZIUM\\.TABLEA,DEBEZIUM\\.TABLEB");
        Mockito.when(config.tableExcludeList()).thenReturn(null);

        String schema = "AND (NOT REGEXP_LIKE(SEG_OWNER,'^SCHEMA1$','i') AND NOT REGEXP_LIKE(SEG_OWNER,'^SCHEMA2$','i')) ";
        String table = "AND (REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEA$','i') " +
                "OR REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEB$','i')) ";

        String result = SqlUtils.logMinerContentsQuery(config, USERNAME);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(schema, table));
    }

    @Test
    public void testStatements() {
        SqlUtils.setRac(false);

        String result = SqlUtils.addLogFileStatement("ADD", "FILENAME");
        String expected = "BEGIN sys.dbms_logmnr.add_logfile(LOGFILENAME => 'FILENAME', OPTIONS => ADD);END;";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.databaseSupplementalLoggingMinCheckQuery();
        expected = "SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.tableSupplementalLoggingCheckQuery(new TableId(null, "s", "t"));
        expected = "SELECT 'KEY', LOG_GROUP_TYPE FROM ALL_LOG_GROUPS WHERE OWNER = 's' AND TABLE_NAME = 't'";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.startLogMinerStatement(Scn.valueOf(10L), Scn.valueOf(20L), OracleConnectorConfig.LogMiningStrategy.ONLINE_CATALOG, true);
        expected = "BEGIN sys.dbms_logmnr.start_logmnr(startScn => '10', endScn => '20', " +
                "OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG  + DBMS_LOGMNR.CONTINUOUS_MINE  + DBMS_LOGMNR.NO_ROWID_IN_STMT);END;";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.startLogMinerStatement(Scn.valueOf(10L), Scn.valueOf(20L), OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO, false);
        expected = "BEGIN sys.dbms_logmnr.start_logmnr(startScn => '10', endScn => '20', " +
                "OPTIONS => DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING  + DBMS_LOGMNR.NO_ROWID_IN_STMT);END;";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.truncateTableStatement("table_name");
        expected = "TRUNCATE TABLE table_name";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.diffInDaysQuery(Scn.valueOf(123L));
        expected = "select sysdate - CAST(scn_to_timestamp(123) as date) from dual";
        assertThat(expected.equals(result)).isTrue();
        result = SqlUtils.diffInDaysQuery(null);
        assertThat(result).isNull();

        result = SqlUtils.redoLogStatusQuery();
        expected = "SELECT F.MEMBER, R.STATUS FROM V$LOGFILE F, V$LOG R WHERE F.GROUP# = R.GROUP# ORDER BY 2";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.switchHistoryQuery();
        expected = "SELECT 'TOTAL', COUNT(1) FROM V$ARCHIVED_LOG WHERE FIRST_TIME > TRUNC(SYSDATE)" +
                " AND DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL' AND ROWNUM=1)";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.currentRedoNameQuery();
        expected = "SELECT F.MEMBER FROM V$LOG LOG, V$LOGFILE F  WHERE LOG.GROUP#=F.GROUP# AND LOG.STATUS='CURRENT'";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.currentRedoLogSequenceQuery();
        expected = "SELECT SEQUENCE# FROM V$LOG WHERE STATUS = 'CURRENT'";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.databaseSupplementalLoggingAllCheckQuery();
        expected = "SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.currentScnQuery();
        expected = "SELECT CURRENT_SCN FROM V$DATABASE";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.oldestFirstChangeQuery(Duration.ofHours(0L));
        expected = "SELECT MIN(FIRST_CHANGE#) FROM (SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# FROM V$LOG UNION SELECT MIN(FIRST_CHANGE#)" +
                " AS FIRST_CHANGE# FROM V$ARCHIVED_LOG WHERE DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS" +
                " WHERE STATUS='VALID' AND TYPE='LOCAL' AND ROWNUM=1) )";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.allOnlineLogsQuery();
        expected = "SELECT MIN(F.MEMBER) AS FILE_NAME, L.NEXT_CHANGE# AS NEXT_CHANGE, F.GROUP#, L.FIRST_CHANGE# AS FIRST_CHANGE, L.STATUS " +
                " FROM V$LOG L, V$LOGFILE F " +
                " WHERE F.GROUP# = L.GROUP# AND L.NEXT_CHANGE# > 0 " +
                " GROUP BY F.GROUP#, L.NEXT_CHANGE#, L.FIRST_CHANGE#, L.STATUS ORDER BY 3";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.tableExistsQuery("table_name");
        expected = "SELECT '1' AS ONE FROM USER_TABLES WHERE TABLE_NAME = 'table_name'";
        assertThat(result).isEqualTo(expected);

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
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.archiveLogsQuery(Scn.valueOf(10L), Duration.ofHours(0L));
        expected = "SELECT NAME AS FILE_NAME, NEXT_CHANGE# AS NEXT_CHANGE, FIRST_CHANGE# AS FIRST_CHANGE FROM V$ARCHIVED_LOG " +
                "WHERE NAME IS NOT NULL AND ARCHIVED = 'YES' " +
                "AND STATUS = 'A' AND NEXT_CHANGE# > 10 " +
                "AND DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL' AND ROWNUM=1) " +
                "ORDER BY 2";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.archiveLogsQuery(Scn.valueOf(10L), Duration.ofHours(1L));
        expected = "SELECT NAME AS FILE_NAME, NEXT_CHANGE# AS NEXT_CHANGE, FIRST_CHANGE# AS FIRST_CHANGE FROM V$ARCHIVED_LOG " +
                "WHERE NAME IS NOT NULL AND ARCHIVED = 'YES' " +
                "AND STATUS = 'A' AND NEXT_CHANGE# > 10 " +
                "AND DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL' AND ROWNUM=1) " +
                "AND FIRST_TIME >= SYSDATE - (1/24) " +
                "ORDER BY 2";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.deleteLogFileStatement("file_name");
        expected = "BEGIN SYS.DBMS_LOGMNR.REMOVE_LOGFILE(LOGFILENAME => 'file_name');END;";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.dropTableStatement("table_name");
        expected = "DROP TABLE TABLE_NAME PURGE";
        assertThat(result).isEqualTo(expected);

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

    private String resolveLogMineryContentQueryFromTemplate(String schemaReplacement, String tableReplacement) {
        String query = LOG_MINER_CONTENT_QUERY_TEMPLATE;

        if (!OracleConnectorConfig.EXCLUDED_SCHEMAS.isEmpty()) {
            StringBuilder systemPredicate = new StringBuilder();
            systemPredicate.append("AND SEG_OWNER NOT IN (");
            for (Iterator<String> i = OracleConnectorConfig.EXCLUDED_SCHEMAS.iterator(); i.hasNext();) {
                String excludedSchema = i.next();
                systemPredicate.append("'").append(excludedSchema.toUpperCase()).append("'");
                if (i.hasNext()) {
                    systemPredicate.append(",");
                }
            }
            systemPredicate.append(") ");
            query = query.replace("${systemTablePredicate}", systemPredicate.toString());
        }
        else {
            query = query.replace("${systemTablePredicate}", "");
        }

        query = query.replace("${schemaPredicate}", schemaReplacement == null ? "" : schemaReplacement);
        query = query.replace("${tablePredicate}", tableReplacement == null ? "" : tableReplacement);
        query = query.replace("${user}", USERNAME);
        return query;
    }
}
