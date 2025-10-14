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

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName;

@SkipWhenAdapterNameIsNot(value = AdapterName.ANY_LOGMINER)
public class SqlUtilsTest {

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    @Test
    public void testDatabaseSupplementalLogMinCheckSql() {
        String result = SqlUtils.databaseSupplementalLoggingMinCheckQuery();
        String expected = "SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE";
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testDatabaseSupplementalLogMinCheckSqlStandardMode() {
        String result = SqlUtils.databaseSupplementalLoggingMinCheckQuery(false);
        String expected = "SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE";
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testDatabaseSupplementalLogMinCheckSqlAdbMode() {
        String result = SqlUtils.databaseSupplementalLoggingMinCheckQuery(true);
        String expected = "SELECT 'KEY', MINIMAL FROM DBA_SUPPLEMENTAL_LOGGING";
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testTableSupplementalLogCheckSql() {
        String result = SqlUtils.tableSupplementalLoggingCheckQuery();
        String expected = "SELECT 'KEY', LOG_GROUP_TYPE FROM ALL_LOG_GROUPS WHERE OWNER=? AND TABLE_NAME=?";
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testScnByTimeDeltaSql() {
        String result = SqlUtils.getScnByTimeDeltaQuery(Scn.valueOf(123L), Duration.ofMinutes(1));
        String expected = "select timestamp_to_scn(CAST(scn_to_timestamp(123) as date) - INTERVAL '1' MINUTE) from dual";
        assertThat(expected.equals(result)).isTrue();
        result = SqlUtils.getScnByTimeDeltaQuery(null, Duration.ofMinutes(1));
        assertThat(result).isNull();
    }

    @Test
    public void testRedoLogStatusSql() {
        String result = SqlUtils.redoLogStatusQuery();
        String expected = "SELECT F.MEMBER, R.STATUS FROM V$LOGFILE F, V$LOG R WHERE F.GROUP# = R.GROUP# ORDER BY 2";
        assertThat(expected.equals(result)).isTrue();
    }

    @Test
    public void testLogSwitchHistorySql() {
        String result = SqlUtils.switchHistoryQuery(null);
        String expected = "SELECT 'TOTAL', COUNT(1) FROM V$ARCHIVED_LOG WHERE FIRST_TIME > TRUNC(SYSDATE)" +
                " AND DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL' AND ROWNUM=1)";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.switchHistoryQuery("LOG_ARCHIVE_DEST_4");
        expected = "SELECT 'TOTAL', COUNT(1) FROM V$ARCHIVED_LOG WHERE FIRST_TIME > TRUNC(SYSDATE)" +
                " AND DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL' AND UPPER(DEST_NAME)='LOG_ARCHIVE_DEST_4')";
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testLogSwitchHistorySqlStandardMode() {
        String result = SqlUtils.switchHistoryQuery(null, false);
        String expected = "SELECT 'TOTAL', COUNT(1) FROM V$ARCHIVED_LOG WHERE FIRST_TIME > TRUNC(SYSDATE)" +
                " AND DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL' AND ROWNUM=1)";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.switchHistoryQuery("LOG_ARCHIVE_DEST_4", false);
        expected = "SELECT 'TOTAL', COUNT(1) FROM V$ARCHIVED_LOG WHERE FIRST_TIME > TRUNC(SYSDATE)" +
                " AND DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL' AND UPPER(DEST_NAME)='LOG_ARCHIVE_DEST_4')";
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testLogSwitchHistorySqlAdbMode() {
        String result = SqlUtils.switchHistoryQuery(null, true);
        String expected = "SELECT 'TOTAL', COUNT(1) FROM V$ARCHIVED_LOG WHERE FIRST_TIME > TRUNC(SYSDATE)" +
                " AND DEST_ID IN (SELECT DISTINCT DEST_ID FROM V$ARCHIVED_LOG)";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.switchHistoryQuery("LOG_ARCHIVE_DEST_4", true);
        expected = "SELECT 'TOTAL', COUNT(1) FROM V$ARCHIVED_LOG WHERE FIRST_TIME > TRUNC(SYSDATE)" +
                " AND DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL' AND UPPER(DEST_NAME)='LOG_ARCHIVE_DEST_4')";
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testCurrentRedoLogFileNameSql() {
        String result = SqlUtils.currentRedoNameQuery();
        String expected = "SELECT F.MEMBER FROM V$LOG LOG, V$LOGFILE F  WHERE LOG.GROUP#=F.GROUP# AND LOG.STATUS='CURRENT'";
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testCurrentRedoLogSequenceSql() {
        String result = SqlUtils.currentRedoLogSequenceQuery();
        String expected = "SELECT SEQUENCE# FROM V$LOG WHERE STATUS = 'CURRENT' ORDER BY SEQUENCE#";
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testDatabaseSupplementalLogAllCheckSql() {
        String result = SqlUtils.databaseSupplementalLoggingAllCheckQuery();
        String expected = "SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE";
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testDatabaseSupplementalLogAllCheckSqlStandardMode() {
        String result = SqlUtils.databaseSupplementalLoggingAllCheckQuery(false);
        String expected = "SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE";
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testDatabaseSupplementalLogAllCheckSqlAdbMode() {
        String result = SqlUtils.databaseSupplementalLoggingAllCheckQuery(true);
        String expected = "SELECT 'KEY', ALL_COLUMN FROM DBA_SUPPLEMENTAL_LOGGING";
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testOldestFirstArchiveLogChangeSql() {
        final String sqlStem = "SELECT MIN(FIRST_CHANGE#) " +
                "FROM (" +
                "SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# " +
                "FROM V$LOG " +
                "UNION " +
                "SELECT MIN(A.FIRST_CHANGE#) AS FIRST_CHANGE# " +
                "FROM V$ARCHIVED_LOG A, V$DATABASE D " +
                "WHERE A.DEST_ID IN (" +
                "SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS " +
                "WHERE STATUS='VALID' " +
                "AND TYPE='LOCAL' " +
                "AND %s) " +
                "AND A.STATUS='A' " +
                "AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# " +
                "AND A.RESETLOGS_TIME = D.RESETLOGS_TIME" +
                "%s)";

        String result = SqlUtils.oldestFirstChangeQuery(Duration.ofHours(0L), null);
        assertThat(result).isEqualTo(String.format(sqlStem, "ROWNUM=1", ""));

        result = SqlUtils.oldestFirstChangeQuery(Duration.ofHours(1L), null);
        assertThat(result).isEqualTo(String.format(sqlStem, "ROWNUM=1", " AND A.FIRST_TIME >= SYSDATE - (1/24)"));

        result = SqlUtils.oldestFirstChangeQuery(Duration.ofHours(0L), "LOG_ARCHIVE_DEST_3");
        assertThat(result).isEqualTo(String.format(sqlStem, "UPPER(DEST_NAME)='LOG_ARCHIVE_DEST_3'", ""));

        result = SqlUtils.oldestFirstChangeQuery(Duration.ofHours(1L), "LOG_ARCHIVE_DEST_3");
        assertThat(result).isEqualTo(String.format(sqlStem, "UPPER(DEST_NAME)='LOG_ARCHIVE_DEST_3'", " AND A.FIRST_TIME >= SYSDATE - (1/24)"));
    }

    @Test
    public void testOldestFirstArchiveLogChangeSqlStandardMode() {
        final String sqlStem = "SELECT MIN(FIRST_CHANGE#) " +
                "FROM (" +
                "SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# " +
                "FROM V$LOG " +
                "UNION " +
                "SELECT MIN(A.FIRST_CHANGE#) AS FIRST_CHANGE# " +
                "FROM V$ARCHIVED_LOG A, V$DATABASE D " +
                "WHERE A.DEST_ID IN (" +
                "SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS " +
                "WHERE STATUS='VALID' " +
                "AND TYPE='LOCAL' " +
                "AND %s) " +
                "AND A.STATUS='A' " +
                "AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# " +
                "AND A.RESETLOGS_TIME = D.RESETLOGS_TIME" +
                "%s)";

        String result = SqlUtils.oldestFirstChangeQuery(Duration.ofHours(0L), null, false);
        assertThat(result).isEqualTo(String.format(sqlStem, "ROWNUM=1", ""));

        result = SqlUtils.oldestFirstChangeQuery(Duration.ofHours(1L), null, false);
        assertThat(result).isEqualTo(String.format(sqlStem, "ROWNUM=1", " AND A.FIRST_TIME >= SYSDATE - (1/24)"));

        result = SqlUtils.oldestFirstChangeQuery(Duration.ofHours(0L), "LOG_ARCHIVE_DEST_3", false);
        assertThat(result).isEqualTo(String.format(sqlStem, "UPPER(DEST_NAME)='LOG_ARCHIVE_DEST_3'", ""));

        result = SqlUtils.oldestFirstChangeQuery(Duration.ofHours(1L), "LOG_ARCHIVE_DEST_3", false);
        assertThat(result).isEqualTo(String.format(sqlStem, "UPPER(DEST_NAME)='LOG_ARCHIVE_DEST_3'", " AND A.FIRST_TIME >= SYSDATE - (1/24)"));
    }

    @Test
    public void testOldestFirstArchiveLogChangeSqlAdbMode() {
        final String sqlStemAdb = "SELECT MIN(FIRST_CHANGE#) " +
                "FROM (" +
                "SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# " +
                "FROM V$LOG " +
                "UNION " +
                "SELECT MIN(A.FIRST_CHANGE#) AS FIRST_CHANGE# " +
                "FROM V$ARCHIVED_LOG A, V$DATABASE D " +
                "WHERE A.DEST_ID IN (" +
                "SELECT DISTINCT DEST_ID FROM V$ARCHIVED_LOG) " +
                "AND A.STATUS='A' " +
                "AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# " +
                "AND A.RESETLOGS_TIME = D.RESETLOGS_TIME" +
                "%s)";

        final String sqlStemAdbWithDestName = "SELECT MIN(FIRST_CHANGE#) " +
                "FROM (" +
                "SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# " +
                "FROM V$LOG " +
                "UNION " +
                "SELECT MIN(A.FIRST_CHANGE#) AS FIRST_CHANGE# " +
                "FROM V$ARCHIVED_LOG A, V$DATABASE D " +
                "WHERE A.DEST_ID IN (" +
                "SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS " +
                "WHERE STATUS='VALID' " +
                "AND TYPE='LOCAL' " +
                "AND UPPER(DEST_NAME)='LOG_ARCHIVE_DEST_3') " +
                "AND A.STATUS='A' " +
                "AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# " +
                "AND A.RESETLOGS_TIME = D.RESETLOGS_TIME" +
                "%s)";

        String result = SqlUtils.oldestFirstChangeQuery(Duration.ofHours(0L), null, true);
        assertThat(result).isEqualTo(String.format(sqlStemAdb, ""));

        result = SqlUtils.oldestFirstChangeQuery(Duration.ofHours(1L), null, true);
        assertThat(result).isEqualTo(String.format(sqlStemAdb, " AND A.FIRST_TIME >= SYSDATE - (1/24)"));

        result = SqlUtils.oldestFirstChangeQuery(Duration.ofHours(0L), "LOG_ARCHIVE_DEST_3", true);
        assertThat(result).isEqualTo(String.format(sqlStemAdbWithDestName, ""));

        result = SqlUtils.oldestFirstChangeQuery(Duration.ofHours(1L), "LOG_ARCHIVE_DEST_3", true);
        assertThat(result).isEqualTo(String.format(sqlStemAdbWithDestName, " AND A.FIRST_TIME >= SYSDATE - (1/24)"));
    }

    @Test
    public void testAllRedoThreadArchiveLogsSql() {
        final String sqlStem = "SELECT A.NAME, A.SEQUENCE#, A.FIRST_CHANGE#, A.NEXT_CHANGE# " +
                "FROM V$ARCHIVED_LOG A, V$DATABASE D " +
                "WHERE A.DEST_ID IN (" +
                "SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL' AND %s) " +
                "AND A.STATUS='A' " +
                "AND A.THREAD#=%d " +
                "AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# " +
                "AND A.RESETLOGS_TIME = D.RESETLOGS_TIME " +
                "ORDER BY A.SEQUENCE# DESC";

        String result = SqlUtils.allRedoThreadArchiveLogs(1, null);
        assertThat(result).isEqualTo(String.format(sqlStem, "ROWNUM=1", 1));

        result = SqlUtils.allRedoThreadArchiveLogs(2, "LOG_ARCHIVE_DEST_5");
        assertThat(result).isEqualTo(String.format(sqlStem, "UPPER(DEST_NAME)='LOG_ARCHIVE_DEST_5'", 2));
    }

    @Test
    public void testAllRedoThreadArchiveLogsSqlStandardMode() {
        final String sqlStem = "SELECT A.NAME, A.SEQUENCE#, A.FIRST_CHANGE#, A.NEXT_CHANGE# " +
                "FROM V$ARCHIVED_LOG A, V$DATABASE D " +
                "WHERE A.DEST_ID IN (" +
                "SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL' AND %s) " +
                "AND A.STATUS='A' " +
                "AND A.THREAD#=%d " +
                "AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# " +
                "AND A.RESETLOGS_TIME = D.RESETLOGS_TIME " +
                "ORDER BY A.SEQUENCE# DESC";

        String result = SqlUtils.allRedoThreadArchiveLogs(1, null, false);
        assertThat(result).isEqualTo(String.format(sqlStem, "ROWNUM=1", 1));

        result = SqlUtils.allRedoThreadArchiveLogs(2, "LOG_ARCHIVE_DEST_5", false);
        assertThat(result).isEqualTo(String.format(sqlStem, "UPPER(DEST_NAME)='LOG_ARCHIVE_DEST_5'", 2));
    }

    @Test
    public void testAllRedoThreadArchiveLogsSqlAdbMode() {
        final String sqlStemAdb = "SELECT A.NAME, A.SEQUENCE#, A.FIRST_CHANGE#, A.NEXT_CHANGE# " +
                "FROM V$ARCHIVED_LOG A, V$DATABASE D " +
                "WHERE A.DEST_ID IN (" +
                "SELECT DISTINCT DEST_ID FROM V$ARCHIVED_LOG) " +
                "AND A.STATUS='A' " +
                "AND A.THREAD#=%d " +
                "AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# " +
                "AND A.RESETLOGS_TIME = D.RESETLOGS_TIME " +
                "ORDER BY A.SEQUENCE# DESC";

        final String sqlStemAdbWithDestName = "SELECT A.NAME, A.SEQUENCE#, A.FIRST_CHANGE#, A.NEXT_CHANGE# " +
                "FROM V$ARCHIVED_LOG A, V$DATABASE D " +
                "WHERE A.DEST_ID IN (" +
                "SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL' AND UPPER(DEST_NAME)='LOG_ARCHIVE_DEST_5') " +
                "AND A.STATUS='A' " +
                "AND A.THREAD#=%d " +
                "AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# " +
                "AND A.RESETLOGS_TIME = D.RESETLOGS_TIME " +
                "ORDER BY A.SEQUENCE# DESC";

        String result = SqlUtils.allRedoThreadArchiveLogs(1, null, true);
        assertThat(result).isEqualTo(String.format(sqlStemAdb, 1));

        result = SqlUtils.allRedoThreadArchiveLogs(2, "LOG_ARCHIVE_DEST_5", true);
        assertThat(result).isEqualTo(String.format(sqlStemAdbWithDestName, 2));
    }

    @Test
    public void testAllMinableLogsSql() {
        final String onlineLogsStem = "SELECT MIN(F.MEMBER) AS FILE_NAME, L.FIRST_CHANGE# FIRST_CHANGE, " +
                "L.NEXT_CHANGE# NEXT_CHANGE, L.ARCHIVED, L.STATUS, 'ONLINE' AS TYPE, L.SEQUENCE# AS SEQ, " +
                "'NO' AS DICT_START, 'NO' AS DICT_END, L.THREAD# AS THREAD " +
                "FROM V$LOGFILE F, V$DATABASE D, V$LOG L " +
                "LEFT JOIN V$ARCHIVED_LOG A " +
                "ON A.FIRST_CHANGE# = L.FIRST_CHANGE# AND A.NEXT_CHANGE# = L.NEXT_CHANGE# " +
                "WHERE ((A.STATUS <> 'A' " +
                "AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# AND A.RESETLOGS_TIME = D.RESETLOGS_TIME) " +
                "OR A.FIRST_CHANGE# IS NULL) " +
                "AND L.STATUS != 'UNUSED' " +
                "AND F.GROUP# = L.GROUP# " +
                "GROUP BY F.GROUP#, L.FIRST_CHANGE#, L.NEXT_CHANGE#, L.STATUS, L.ARCHIVED, L.SEQUENCE#, L.THREAD#";

        final String archiveLogsStem = "SELECT A.NAME AS FILE_NAME, A.FIRST_CHANGE# FIRST_CHANGE, " +
                "A.NEXT_CHANGE# NEXT_CHANGE, 'YES', NULL, 'ARCHIVED', A.SEQUENCE# AS SEQ, " +
                "A.DICTIONARY_BEGIN, A.DICTIONARY_END, A.THREAD# AS THREAD " +
                "FROM V$ARCHIVED_LOG A, V$DATABASE D " +
                "WHERE A.NAME IS NOT NULL " +
                "AND A.ARCHIVED = 'YES' " +
                "AND A.STATUS = 'A' " +
                "AND A.NEXT_CHANGE# > %d " +
                "AND A.DEST_ID IN (" +
                "SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL' AND %s) " +
                "AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# " +
                "AND A.RESETLOGS_TIME = D.RESETLOGS_TIME " +
                "%s" +
                "ORDER BY 7";

        final String combinedStem = onlineLogsStem + " UNION " + archiveLogsStem;

        String result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(0L), false, null);
        assertThat(result).isEqualTo(String.format(combinedStem, 10, "ROWNUM=1", ""));

        result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(0L), false, "LOG_ARCHIVE_DEST_2");
        assertThat(result).isEqualTo(String.format(combinedStem, 10, "UPPER(DEST_NAME)='LOG_ARCHIVE_DEST_2'", ""));

        result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(0L), true, null);
        assertThat(result).isEqualTo(String.format(archiveLogsStem, 10, "ROWNUM=1", ""));

        result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(1L), false, null);
        assertThat(result).isEqualTo(String.format(combinedStem, 10, "ROWNUM=1", "AND A.FIRST_TIME >= SYSDATE - (1/24) "));

        result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(1L), true, null);
        assertThat(result).isEqualTo(String.format(archiveLogsStem, 10, "ROWNUM=1", "AND A.FIRST_TIME >= SYSDATE - (1/24) "));
    }

    @Test
    public void testAllMinableLogsSqlStandardMode() {
        final String onlineLogsStem = "SELECT MIN(F.MEMBER) AS FILE_NAME, L.FIRST_CHANGE# FIRST_CHANGE, " +
                "L.NEXT_CHANGE# NEXT_CHANGE, L.ARCHIVED, L.STATUS, 'ONLINE' AS TYPE, L.SEQUENCE# AS SEQ, " +
                "'NO' AS DICT_START, 'NO' AS DICT_END, L.THREAD# AS THREAD " +
                "FROM V$LOGFILE F, V$DATABASE D, V$LOG L " +
                "LEFT JOIN V$ARCHIVED_LOG A " +
                "ON A.FIRST_CHANGE# = L.FIRST_CHANGE# AND A.NEXT_CHANGE# = L.NEXT_CHANGE# " +
                "WHERE ((A.STATUS <> 'A' " +
                "AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# AND A.RESETLOGS_TIME = D.RESETLOGS_TIME) " +
                "OR A.FIRST_CHANGE# IS NULL) " +
                "AND L.STATUS != 'UNUSED' " +
                "AND F.GROUP# = L.GROUP# " +
                "GROUP BY F.GROUP#, L.FIRST_CHANGE#, L.NEXT_CHANGE#, L.STATUS, L.ARCHIVED, L.SEQUENCE#, L.THREAD#";

        final String archiveLogsStem = "SELECT A.NAME AS FILE_NAME, A.FIRST_CHANGE# FIRST_CHANGE, " +
                "A.NEXT_CHANGE# NEXT_CHANGE, 'YES', NULL, 'ARCHIVED', A.SEQUENCE# AS SEQ, " +
                "A.DICTIONARY_BEGIN, A.DICTIONARY_END, A.THREAD# AS THREAD " +
                "FROM V$ARCHIVED_LOG A, V$DATABASE D " +
                "WHERE A.NAME IS NOT NULL " +
                "AND A.ARCHIVED = 'YES' " +
                "AND A.STATUS = 'A' " +
                "AND A.NEXT_CHANGE# > %d " +
                "AND A.DEST_ID IN (" +
                "SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL' AND %s) " +
                "AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# " +
                "AND A.RESETLOGS_TIME = D.RESETLOGS_TIME " +
                "%s" +
                "ORDER BY 7";

        final String combinedStem = onlineLogsStem + " UNION " + archiveLogsStem;

        String result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(0L), false, null, false);
        assertThat(result).isEqualTo(String.format(combinedStem, 10, "ROWNUM=1", ""));

        result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(0L), false, "LOG_ARCHIVE_DEST_2", false);
        assertThat(result).isEqualTo(String.format(combinedStem, 10, "UPPER(DEST_NAME)='LOG_ARCHIVE_DEST_2'", ""));

        result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(0L), true, null, false);
        assertThat(result).isEqualTo(String.format(archiveLogsStem, 10, "ROWNUM=1", ""));

        result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(1L), false, null, false);
        assertThat(result).isEqualTo(String.format(combinedStem, 10, "ROWNUM=1", "AND A.FIRST_TIME >= SYSDATE - (1/24) "));

        result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(1L), true, null, false);
        assertThat(result).isEqualTo(String.format(archiveLogsStem, 10, "ROWNUM=1", "AND A.FIRST_TIME >= SYSDATE - (1/24) "));
    }

    @Test
    public void testAllMinableLogsSqlAdbMode() {
        final String onlineLogsStem = "SELECT MIN(F.MEMBER) AS FILE_NAME, L.FIRST_CHANGE# FIRST_CHANGE, " +
                "L.NEXT_CHANGE# NEXT_CHANGE, L.ARCHIVED, L.STATUS, 'ONLINE' AS TYPE, L.SEQUENCE# AS SEQ, " +
                "'NO' AS DICT_START, 'NO' AS DICT_END, L.THREAD# AS THREAD " +
                "FROM V$LOGFILE F, V$DATABASE D, V$LOG L " +
                "LEFT JOIN V$ARCHIVED_LOG A " +
                "ON A.FIRST_CHANGE# = L.FIRST_CHANGE# AND A.NEXT_CHANGE# = L.NEXT_CHANGE# " +
                "WHERE ((A.STATUS <> 'A' " +
                "AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# AND A.RESETLOGS_TIME = D.RESETLOGS_TIME) " +
                "OR A.FIRST_CHANGE# IS NULL) " +
                "AND L.STATUS != 'UNUSED' " +
                "AND F.GROUP# = L.GROUP# " +
                "GROUP BY F.GROUP#, L.FIRST_CHANGE#, L.NEXT_CHANGE#, L.STATUS, L.ARCHIVED, L.SEQUENCE#, L.THREAD#";

        final String archiveLogsStemAdb = "SELECT A.NAME AS FILE_NAME, A.FIRST_CHANGE# FIRST_CHANGE, " +
                "A.NEXT_CHANGE# NEXT_CHANGE, 'YES', NULL, 'ARCHIVED', A.SEQUENCE# AS SEQ, " +
                "A.DICTIONARY_BEGIN, A.DICTIONARY_END, A.THREAD# AS THREAD " +
                "FROM V$ARCHIVED_LOG A, V$DATABASE D " +
                "WHERE A.NAME IS NOT NULL " +
                "AND A.ARCHIVED = 'YES' " +
                "AND A.STATUS = 'A' " +
                "AND A.NEXT_CHANGE# > %d " +
                "AND A.DEST_ID IN (" +
                "SELECT DISTINCT DEST_ID FROM V$ARCHIVED_LOG) " +
                "AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# " +
                "AND A.RESETLOGS_TIME = D.RESETLOGS_TIME " +
                "%s" +
                "ORDER BY 7";

        final String archiveLogsStemAdbWithDestName = "SELECT A.NAME AS FILE_NAME, A.FIRST_CHANGE# FIRST_CHANGE, " +
                "A.NEXT_CHANGE# NEXT_CHANGE, 'YES', NULL, 'ARCHIVED', A.SEQUENCE# AS SEQ, " +
                "A.DICTIONARY_BEGIN, A.DICTIONARY_END, A.THREAD# AS THREAD " +
                "FROM V$ARCHIVED_LOG A, V$DATABASE D " +
                "WHERE A.NAME IS NOT NULL " +
                "AND A.ARCHIVED = 'YES' " +
                "AND A.STATUS = 'A' " +
                "AND A.NEXT_CHANGE# > %d " +
                "AND A.DEST_ID IN (" +
                "SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL' AND UPPER(DEST_NAME)='LOG_ARCHIVE_DEST_2') " +
                "AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# " +
                "AND A.RESETLOGS_TIME = D.RESETLOGS_TIME " +
                "%s" +
                "ORDER BY 7";

        final String combinedStemAdb = onlineLogsStem + " UNION " + archiveLogsStemAdb;
        final String combinedStemAdbWithDestName = onlineLogsStem + " UNION " + archiveLogsStemAdbWithDestName;

        // ADB mode with null destination and archiveLogOnlyMode=false (includes UNION with online logs)
        String result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(0L), false, null, true);
        assertThat(result).isEqualTo(String.format(combinedStemAdb, 10, ""));

        // ADB mode with null destination and archiveLogOnlyMode=true (no UNION, archive logs only)
        result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(0L), true, null, true);
        assertThat(result).isEqualTo(String.format(archiveLogsStemAdb, 10, ""));

        // ADB mode with specific destination name (still uses V$ARCHIVE_DEST_STATUS)
        result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(0L), false, "LOG_ARCHIVE_DEST_2", true);
        assertThat(result).isEqualTo(String.format(combinedStemAdbWithDestName, 10, ""));

        // ADB mode with archive log retention and archiveLogOnlyMode=false
        result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(1L), false, null, true);
        assertThat(result).isEqualTo(String.format(combinedStemAdb, 10, "AND A.FIRST_TIME >= SYSDATE - (1/24) "));

        // ADB mode with archive log retention and archiveLogOnlyMode=true
        result = SqlUtils.allMinableLogsQuery(Scn.valueOf(10L), Duration.ofHours(1L), true, null, true);
        assertThat(result).isEqualTo(String.format(archiveLogsStemAdb, 10, "AND A.FIRST_TIME >= SYSDATE - (1/24) "));
    }

}
