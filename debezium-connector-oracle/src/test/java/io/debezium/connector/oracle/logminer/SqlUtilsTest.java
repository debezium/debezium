/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName;

@SkipWhenAdapterNameIsNot(value = AdapterName.LOGMINER)
public class SqlUtilsTest {

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    @Test
    public void testStatements() {
        String result = SqlUtils.databaseSupplementalLoggingMinCheckQuery();
        String expected = "SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.tableSupplementalLoggingCheckQuery();
        expected = "SELECT 'KEY', LOG_GROUP_TYPE FROM ALL_LOG_GROUPS WHERE OWNER=? AND TABLE_NAME=?";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.startLogMinerStatement(Scn.valueOf(10L), Scn.valueOf(20L), OracleConnectorConfig.LogMiningStrategy.ONLINE_CATALOG, true, null);
        expected = "BEGIN sys.dbms_logmnr.start_logmnr(startScn => '10', endScn => '20', " +
                "OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.CONTINUOUS_MINE + DBMS_LOGMNR.NO_ROWID_IN_STMT);END;";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.startLogMinerStatement(Scn.valueOf(10L), Scn.valueOf(20L), OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO, false, null);
        expected = "BEGIN sys.dbms_logmnr.start_logmnr(startScn => '10', endScn => '20', " +
                "OPTIONS => DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING + DBMS_LOGMNR.NO_ROWID_IN_STMT);END;";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.startLogMinerStatement(Scn.valueOf(10L), Scn.valueOf(20L), OracleConnectorConfig.LogMiningStrategy.DICTIONARY_FROM_FILE, false,
                "/u01/dictionary.file");
        expected = "BEGIN sys.dbms_logmnr.start_logmnr(startScn => '10', endScn => '20', " +
                "OPTIONS => DBMS_LOGMNR.NO_ROWID_IN_STMT, DICTFILENAME => '/u01/dictionary.file');END;";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.getScnByTimeDeltaQuery(Scn.valueOf(123L), Duration.ofMinutes(1));
        expected = "select timestamp_to_scn(CAST(scn_to_timestamp(123) as date) - INTERVAL '1' MINUTE) from dual";
        assertThat(expected.equals(result)).isTrue();
        result = SqlUtils.getScnByTimeDeltaQuery(null, Duration.ofMinutes(1));
        assertThat(result).isNull();

        result = SqlUtils.redoLogStatusQuery();
        expected = "SELECT F.MEMBER, R.STATUS FROM V$LOGFILE F, V$LOG R WHERE F.GROUP# = R.GROUP# ORDER BY 2";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.switchHistoryQuery(null);
        expected = "SELECT 'TOTAL', COUNT(1) FROM V$ARCHIVED_LOG WHERE FIRST_TIME > TRUNC(SYSDATE)" +
                " AND DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL' AND ROWNUM=1)";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.switchHistoryQuery("LOG_ARCHIVE_DEST_4");
        expected = "SELECT 'TOTAL', COUNT(1) FROM V$ARCHIVED_LOG WHERE FIRST_TIME > TRUNC(SYSDATE)" +
                " AND DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL' AND UPPER(DEST_NAME)='LOG_ARCHIVE_DEST_4')";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.currentRedoNameQuery();
        expected = "SELECT F.MEMBER FROM V$LOG LOG, V$LOGFILE F  WHERE LOG.GROUP#=F.GROUP# AND LOG.STATUS='CURRENT'";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.currentRedoLogSequenceQuery();
        expected = "SELECT SEQUENCE# FROM V$LOG WHERE STATUS = 'CURRENT' ORDER BY SEQUENCE#";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.databaseSupplementalLoggingAllCheckQuery();
        expected = "SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.oldestFirstChangeQuery(Duration.ofHours(0L), null);
        expected = "SELECT MIN(FIRST_CHANGE#) FROM (SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# FROM V$LOG UNION SELECT MIN(FIRST_CHANGE#)" +
                " AS FIRST_CHANGE# FROM V$ARCHIVED_LOG WHERE DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS" +
                " WHERE STATUS='VALID' AND TYPE='LOCAL' AND ROWNUM=1) AND STATUS='A')";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.oldestFirstChangeQuery(Duration.ofHours(1L), null);
        expected = "SELECT MIN(FIRST_CHANGE#) FROM (SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# FROM V$LOG UNION SELECT MIN(FIRST_CHANGE#)" +
                " AS FIRST_CHANGE# FROM V$ARCHIVED_LOG WHERE DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS" +
                " WHERE STATUS='VALID' AND TYPE='LOCAL' AND ROWNUM=1) AND STATUS='A' AND FIRST_TIME >= SYSDATE - (1/24))";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.oldestFirstChangeQuery(Duration.ofHours(0L), "LOG_ARCHIVE_DEST_3");
        expected = "SELECT MIN(FIRST_CHANGE#) FROM (SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# FROM V$LOG UNION SELECT MIN(FIRST_CHANGE#)" +
                " AS FIRST_CHANGE# FROM V$ARCHIVED_LOG WHERE DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS" +
                " WHERE STATUS='VALID' AND TYPE='LOCAL' AND UPPER(DEST_NAME)='LOG_ARCHIVE_DEST_3') AND STATUS='A')";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.oldestFirstChangeQuery(Duration.ofHours(1L), "LOG_ARCHIVE_DEST_3");
        expected = "SELECT MIN(FIRST_CHANGE#) FROM (SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# FROM V$LOG UNION SELECT MIN(FIRST_CHANGE#)" +
                " AS FIRST_CHANGE# FROM V$ARCHIVED_LOG WHERE DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS" +
                " WHERE STATUS='VALID' AND TYPE='LOCAL' AND UPPER(DEST_NAME)='LOG_ARCHIVE_DEST_3') AND STATUS='A' AND FIRST_TIME >= SYSDATE - (1/24))";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(0L), false, null);
        expected = "SELECT MIN(F.MEMBER) AS FILE_NAME, L.FIRST_CHANGE# FIRST_CHANGE, L.NEXT_CHANGE# NEXT_CHANGE, L.ARCHIVED, " +
                "L.STATUS, 'ONLINE' AS TYPE, L.SEQUENCE# AS SEQ, 'NO' AS DICT_START, 'NO' AS DICT_END, L.THREAD# AS THREAD FROM V$LOGFILE F, " +
                "V$LOG L LEFT JOIN V$ARCHIVED_LOG A ON A.FIRST_CHANGE# = L.FIRST_CHANGE# AND A.NEXT_CHANGE# = L.NEXT_CHANGE# " +
                "WHERE (A.STATUS <> 'A' OR A.FIRST_CHANGE# IS NULL) AND L.STATUS != 'UNUSED' AND F.GROUP# = L.GROUP# GROUP BY F.GROUP#, L.FIRST_CHANGE#, L.NEXT_CHANGE#, "
                +
                "L.STATUS, L.ARCHIVED, L.SEQUENCE#, L.THREAD# UNION SELECT A.NAME AS FILE_NAME, A.FIRST_CHANGE# FIRST_CHANGE, " +
                "A.NEXT_CHANGE# NEXT_CHANGE, 'YES', NULL, 'ARCHIVED', A.SEQUENCE# AS SEQ, A.DICTIONARY_BEGIN, " +
                "A.DICTIONARY_END, A.THREAD# AS THREAD FROM V$ARCHIVED_LOG A WHERE A.NAME IS NOT NULL AND A.ARCHIVED = 'YES' AND A.STATUS = 'A' " +
                "AND A.NEXT_CHANGE# > 10 AND A.DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' " +
                "AND TYPE='LOCAL' AND ROWNUM=1) ORDER BY 7";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(0L), false, "LOG_ARCHIVE_DEST_2");
        expected = "SELECT MIN(F.MEMBER) AS FILE_NAME, L.FIRST_CHANGE# FIRST_CHANGE, L.NEXT_CHANGE# NEXT_CHANGE, L.ARCHIVED, " +
                "L.STATUS, 'ONLINE' AS TYPE, L.SEQUENCE# AS SEQ, 'NO' AS DICT_START, 'NO' AS DICT_END, L.THREAD# AS THREAD FROM V$LOGFILE F, " +
                "V$LOG L LEFT JOIN V$ARCHIVED_LOG A ON A.FIRST_CHANGE# = L.FIRST_CHANGE# AND A.NEXT_CHANGE# = L.NEXT_CHANGE# " +
                "WHERE (A.STATUS <> 'A' OR A.FIRST_CHANGE# IS NULL) AND L.STATUS != 'UNUSED' AND F.GROUP# = L.GROUP# GROUP BY F.GROUP#, L.FIRST_CHANGE#, L.NEXT_CHANGE#, "
                +
                "L.STATUS, L.ARCHIVED, L.SEQUENCE#, L.THREAD# UNION SELECT A.NAME AS FILE_NAME, A.FIRST_CHANGE# FIRST_CHANGE, " +
                "A.NEXT_CHANGE# NEXT_CHANGE, 'YES', NULL, 'ARCHIVED', A.SEQUENCE# AS SEQ, A.DICTIONARY_BEGIN, " +
                "A.DICTIONARY_END, A.THREAD# AS THREAD FROM V$ARCHIVED_LOG A WHERE A.NAME IS NOT NULL AND A.ARCHIVED = 'YES' AND A.STATUS = 'A' " +
                "AND A.NEXT_CHANGE# > 10 AND A.DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' " +
                "AND TYPE='LOCAL' AND UPPER(DEST_NAME)='LOG_ARCHIVE_DEST_2') ORDER BY 7";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(0L), true, null);
        expected = "SELECT A.NAME AS FILE_NAME, A.FIRST_CHANGE# FIRST_CHANGE, " +
                "A.NEXT_CHANGE# NEXT_CHANGE, 'YES', NULL, 'ARCHIVED', A.SEQUENCE# AS SEQ, A.DICTIONARY_BEGIN, " +
                "A.DICTIONARY_END, A.THREAD# AS THREAD FROM V$ARCHIVED_LOG A WHERE A.NAME IS NOT NULL AND A.ARCHIVED = 'YES' AND A.STATUS = 'A' " +
                "AND A.NEXT_CHANGE# > 10 AND A.DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' " +
                "AND TYPE='LOCAL' AND ROWNUM=1) ORDER BY 7";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(1L), false, null);
        expected = "SELECT MIN(F.MEMBER) AS FILE_NAME, L.FIRST_CHANGE# FIRST_CHANGE, L.NEXT_CHANGE# NEXT_CHANGE, L.ARCHIVED, " +
                "L.STATUS, 'ONLINE' AS TYPE, L.SEQUENCE# AS SEQ, 'NO' AS DICT_START, 'NO' AS DICT_END, L.THREAD# AS THREAD FROM V$LOGFILE F, " +
                "V$LOG L LEFT JOIN V$ARCHIVED_LOG A ON A.FIRST_CHANGE# = L.FIRST_CHANGE# AND A.NEXT_CHANGE# = L.NEXT_CHANGE# " +
                "WHERE (A.STATUS <> 'A' OR A.FIRST_CHANGE# IS NULL) AND L.STATUS != 'UNUSED' AND F.GROUP# = L.GROUP# GROUP BY F.GROUP#, L.FIRST_CHANGE#, L.NEXT_CHANGE#, "
                +
                "L.STATUS, L.ARCHIVED, L.SEQUENCE#, L.THREAD# UNION SELECT A.NAME AS FILE_NAME, A.FIRST_CHANGE# FIRST_CHANGE, " +
                "A.NEXT_CHANGE# NEXT_CHANGE, 'YES', NULL, 'ARCHIVED', A.SEQUENCE# AS SEQ, A.DICTIONARY_BEGIN, " +
                "A.DICTIONARY_END, A.THREAD# AS THREAD FROM V$ARCHIVED_LOG A WHERE A.NAME IS NOT NULL AND A.ARCHIVED = 'YES' AND A.STATUS = 'A' " +
                "AND A.NEXT_CHANGE# > 10 AND A.DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' " +
                "AND TYPE='LOCAL' AND ROWNUM=1) AND A.FIRST_TIME >= SYSDATE - (1/24) ORDER BY 7";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(1L), true, null);
        expected = "SELECT A.NAME AS FILE_NAME, A.FIRST_CHANGE# FIRST_CHANGE, " +
                "A.NEXT_CHANGE# NEXT_CHANGE, 'YES', NULL, 'ARCHIVED', A.SEQUENCE# AS SEQ, A.DICTIONARY_BEGIN, " +
                "A.DICTIONARY_END, A.THREAD# AS THREAD FROM V$ARCHIVED_LOG A WHERE A.NAME IS NOT NULL AND A.ARCHIVED = 'YES' AND A.STATUS = 'A' " +
                "AND A.NEXT_CHANGE# > 10 AND A.DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' " +
                "AND TYPE='LOCAL' AND ROWNUM=1) AND A.FIRST_TIME >= SYSDATE - (1/24) ORDER BY 7";
        assertThat(result).isEqualTo(expected);
    }
}
