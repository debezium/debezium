/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.time.Duration;

import io.debezium.connector.oracle.Scn;
import io.debezium.util.Strings;

/**
 * This utility class contains SQL statements to configure, manage and query Oracle LogMiner
 *     todo handle INVALID file member (report somehow and continue to work with valid file), handle adding multiplexed files,
 *     todo SELECT name, value FROM v$sysstat WHERE name = 'redo wastage';
 *     todo SELECT GROUP#, STATUS, MEMBER FROM V$LOGFILE WHERE STATUS='INVALID'; (drop and recreate? or do it manually?)
 *     todo table level supplemental logging
 *     todo When you use the SKIP_CORRUPTION option to DBMS_LOGMNR.START_LOGMNR, any corruptions in the redo log files are skipped during select operations from the V$LOGMNR_CONTENTS view.
 *     todo if file is compressed?
 // For every corrupt redo record encountered,
 // a row is returned that contains the value CORRUPTED_BLOCKS in the OPERATION column, 1343 in the STATUS column, and the number of blocks skipped in the INFO column.
 */
public class SqlUtils {

    // ****** RAC specifics *****//
    // https://docs.oracle.com/cd/B28359_01/server.111/b28319/logminer.htm#i1015913
    // https://asktom.oracle.com/pls/asktom/f?p=100:11:0::::P11_QUESTION_ID:18183400346178753
    // We should never read from GV$LOG, GV$LOGFILE, GV$ARCHIVED_LOG, GV$ARCHIVE_DEST_STATUS and GV$LOGMNR_CONTENTS
    // using GV$DATABASE is also misleading
    // Those views are exceptions on RAC system, all corresponding V$ views see entries from all RAC nodes.
    // So reading from GV* will return duplications, do no do it
    // *****************************

    // database system views
    private static final String DATABASE_VIEW = "V$DATABASE";
    private static final String LOG_VIEW = "V$LOG";
    private static final String LOGFILE_VIEW = "V$LOGFILE";
    private static final String ARCHIVED_LOG_VIEW = "V$ARCHIVED_LOG";
    private static final String ARCHIVE_DEST_STATUS_VIEW = "V$ARCHIVE_DEST_STATUS";
    private static final String ALL_LOG_GROUPS = "ALL_LOG_GROUPS";
    private static final String DBA_SUPPLEMENTAL_LOGGING = "DBA_SUPPLEMENTAL_LOGGING";

    public static String redoLogStatusQuery() {
        return String.format("SELECT F.MEMBER, R.STATUS FROM %s F, %s R WHERE F.GROUP# = R.GROUP# ORDER BY 2", LOGFILE_VIEW, LOG_VIEW);
    }

    public static String switchHistoryQuery(String archiveDestinationName) {
        return switchHistoryQuery(archiveDestinationName, false);
    }

    public static String switchHistoryQuery(String archiveDestinationName, boolean autonomousDatabaseMode) {
        return String.format("SELECT 'TOTAL', COUNT(1) FROM %s WHERE FIRST_TIME > TRUNC(SYSDATE)" +
                " AND DEST_ID IN (" + localArchiveLogDestinationsOnlyQuery(archiveDestinationName, autonomousDatabaseMode) + ")",
                ARCHIVED_LOG_VIEW);
    }

    public static String currentRedoNameQuery() {
        return String.format("SELECT F.MEMBER FROM %s LOG, %s F  WHERE LOG.GROUP#=F.GROUP# AND LOG.STATUS='CURRENT'", LOG_VIEW, LOGFILE_VIEW);
    }

    public static String currentRedoLogSequenceQuery() {
        return String.format("SELECT SEQUENCE# FROM %s WHERE STATUS = 'CURRENT' ORDER BY SEQUENCE#", LOG_VIEW);
    }

    public static String databaseSupplementalLoggingAllCheckQuery() {
        return databaseSupplementalLoggingAllCheckQuery(false);
    }

    public static String databaseSupplementalLoggingAllCheckQuery(boolean autonomousDatabaseMode) {
        if (autonomousDatabaseMode) {
            return String.format("SELECT 'KEY', ALL_COLUMN FROM %s", DBA_SUPPLEMENTAL_LOGGING);
        }
        else {
            return String.format("SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_ALL FROM %s", DATABASE_VIEW);
        }
    }

    public static String databaseSupplementalLoggingMinCheckQuery() {
        return databaseSupplementalLoggingMinCheckQuery(false);
    }

    public static String databaseSupplementalLoggingMinCheckQuery(boolean autonomousDatabaseMode) {
        if (autonomousDatabaseMode) {
            return String.format("SELECT 'KEY', MINIMAL FROM %s", DBA_SUPPLEMENTAL_LOGGING);
        }
        else {
            return String.format("SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_MIN FROM %s", DATABASE_VIEW);
        }
    }

    public static String tableSupplementalLoggingCheckQuery() {
        return String.format("SELECT 'KEY', LOG_GROUP_TYPE FROM %s WHERE OWNER=? AND TABLE_NAME=?", ALL_LOG_GROUPS);
    }

    public static String oldestFirstChangeQuery(Duration archiveLogRetention, String archiveDestinationName) {
        return oldestFirstChangeQuery(archiveLogRetention, archiveDestinationName, false);
    }

    public static String oldestFirstChangeQuery(Duration archiveLogRetention, String archiveDestinationName, boolean autonomousDatabaseMode) {
        final StringBuilder sb = new StringBuilder();
        sb.append("SELECT MIN(FIRST_CHANGE#) FROM (SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# ");
        sb.append("FROM ").append(LOG_VIEW).append(" ");
        sb.append("UNION SELECT MIN(A.FIRST_CHANGE#) AS FIRST_CHANGE# ");
        sb.append("FROM ").append(ARCHIVED_LOG_VIEW).append(" A, ").append(DATABASE_VIEW).append(" D ");
        sb.append("WHERE A.DEST_ID IN (").append(localArchiveLogDestinationsOnlyQuery(archiveDestinationName, autonomousDatabaseMode)).append(") ");
        sb.append("AND A.STATUS='A' ");
        sb.append("AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# ");
        sb.append("AND A.RESETLOGS_TIME = D.RESETLOGS_TIME");

        if (!archiveLogRetention.isNegative() && !archiveLogRetention.isZero()) {
            sb.append(" AND A.FIRST_TIME >= SYSDATE - (").append(archiveLogRetention.toHours()).append("/24)");
        }

        return sb.append(")").toString();
    }

    public static String allRedoThreadArchiveLogs(int threadId, String archiveDestinationName) {
        return allRedoThreadArchiveLogs(threadId, archiveDestinationName, false);
    }

    public static String allRedoThreadArchiveLogs(int threadId, String archiveDestinationName, boolean autonomousDatabaseMode) {
        final StringBuilder sb = new StringBuilder();
        sb.append("SELECT A.NAME, A.SEQUENCE#, A.FIRST_CHANGE#, A.NEXT_CHANGE# ");
        sb.append("FROM ").append(ARCHIVED_LOG_VIEW).append(" A, ").append(DATABASE_VIEW).append(" D ");
        sb.append("WHERE A.DEST_ID IN (").append(localArchiveLogDestinationsOnlyQuery(archiveDestinationName, autonomousDatabaseMode)).append(") ");
        sb.append("AND A.STATUS='A' ");
        sb.append("AND A.THREAD#=").append(threadId).append(" ");
        sb.append("AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# ");
        sb.append("AND A.RESETLOGS_TIME = D.RESETLOGS_TIME ");
        sb.append("ORDER BY A.SEQUENCE# DESC");
        return sb.toString();
    }

    /**
     * Obtain a query to fetch all available minable logs, both archive and online redo logs.
     *
     * @param scn oldest system change number to search by
     * @param archiveLogRetention duration archive logs will be mined
     * @param archiveLogOnlyMode true if to only mine archive logs, false to mine all available logs
     * @param archiveDestinationName configured archive log destination to use, may be {@code null}
     * @return the query string to obtain minable log files
     */
    public static String allMinableLogsQuery(Scn scn, Duration archiveLogRetention, boolean archiveLogOnlyMode, String archiveDestinationName) {
        return allMinableLogsQuery(scn, archiveLogRetention, archiveLogOnlyMode, archiveDestinationName, false);
    }

    public static String allMinableLogsQuery(Scn scn, Duration archiveLogRetention, boolean archiveLogOnlyMode, String archiveDestinationName,
                                             boolean autonomousDatabaseMode) {
        // The generated query performs a union in order to obtain a list of all archive logs that should be mined
        // combined with a list of redo logs that should be mined.
        //
        // The first part of the union query generated is as follows:
        //
        // SELECT MIN(F.MEMBER) AS FILE_NAME, L.FIRST_CHANGE# FIRST_CHANGE, L.NEXT_CHANGE# NEXT_CHANGE, L.ARCHIVED,
        // L.STATUS, 'ONLINE' AS TYPE, L.SEQUENCE# AS SEQ, 'NO' AS DICT_START, 'NO' AS DICT_END, L.THREAD# AS THREAD
        // FROM V$LOGFILE F, V$LOG L
        // LEFT JOIN V$ARCHIVED_LOG A
        // ON A.FIRST_CHANGE# = L.FIRST_CHANGE# AND A.NEXT_CHANGE# = L.NEXT_CHANGE#
        // WHERE (A.FIRST_CHANGE# IS NULL OR A.STATUS <> 'A')
        // AND F.GROUP# = L.GROUP#
        // GROUP BY F.GROUP#, L.FIRST_CHANGE#, L.NEXT_CHANGE#, L.STATUS, L.ARCHIVED, L.SEQUENCE#, L.THREAD#
        //
        // The above query joins the redo logs view with the archived logs view, excluding any redo log that has
        // already been archived and has a matching redo log SCN range in the archive logs view. This allows
        // the database to de-duplicate logs between redo and archive based on SCN ranges so we don't need to do
        // this in Java and avoids the need to execute two separate queries that could introduce some state
        // change between them by Oracle.
        //
        // The second part of the union query:
        //
        // SELECT A.NAME AS FILE_NAME, A.FIRST_CHANGE# FIRST_CHANGE, A.NEXT_CHANGE# NEXT_CHANGE, 'YES',
        // NULL, 'ARCHIVED', A.SEQUENCE# AS SEQ, A.DICTIONARY_BEGIN, A.DICTIONARY_END, A.THREAD# AS THREAD
        // FROM V$ARCHIVED_LOG A
        // WHERE A.NAME IS NOT NULL
        // AND A.ARCHIVED = 'YES'
        // AND A.STATUS = 'A'
        // AND A.NEXT_CHANGE# > scn
        // AND A.DEST_ID IN ( SELECT DEST_ID FROM V$ARCHIVED_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL' AND ROWNUM=1)
        // AND A.FIRST_TIME >= START - (hours/24)
        //
        // The above query obtains a list of all available archive logs that should be mined that have an SCN range
        // which either includes or comes after the SCN where mining is to begin. The predicates in this query are
        // to capture only archive logs that are available and haven't been deleted. Additionally the DEST_ID
        // predicate makes sure that if archive logs are being dually written for other Oracle services that we
        // only fetch the local/valid instances. The last predicate is optional and is meant to restrict the
        // archive logs to only those in the past X hours if log.mining.archive.log.hours is greater than 0.
        //
        // Lastly the query applies "ORDER BY 7" to order the results by SEQ (sequence number). Each Oracle log
        // is assigned a unique sequence. This order has no technical impact on LogMiner but its used mainly as
        // a way to make it easier when looking at debug logs to identify gaps in the log sequences when several
        // logs may be added to a single mining session.

        final StringBuilder sb = new StringBuilder();
        if (!archiveLogOnlyMode) {
            sb.append("SELECT MIN(F.MEMBER) AS FILE_NAME, L.FIRST_CHANGE# FIRST_CHANGE, L.NEXT_CHANGE# NEXT_CHANGE, L.ARCHIVED, ");
            sb.append("L.STATUS, 'ONLINE' AS TYPE, L.SEQUENCE# AS SEQ, 'NO' AS DICT_START, 'NO' AS DICT_END, L.THREAD# AS THREAD ");
            sb.append("FROM ").append(LOGFILE_VIEW).append(" F, ");
            sb.append(DATABASE_VIEW).append(" D, ");
            sb.append(LOG_VIEW).append(" L ");
            sb.append("LEFT JOIN ").append(ARCHIVED_LOG_VIEW).append(" A ");
            sb.append("ON A.FIRST_CHANGE# = L.FIRST_CHANGE# AND A.NEXT_CHANGE# = L.NEXT_CHANGE# ");
            sb.append("WHERE ((");
            sb.append("A.STATUS <> 'A' ");
            sb.append("AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# ");
            sb.append("AND A.RESETLOGS_TIME = D.RESETLOGS_TIME) ");
            sb.append("OR A.FIRST_CHANGE# IS NULL) ");
            sb.append("AND L.STATUS != 'UNUSED' ");
            sb.append("AND F.GROUP# = L.GROUP# ");
            sb.append("GROUP BY F.GROUP#, L.FIRST_CHANGE#, L.NEXT_CHANGE#, L.STATUS, L.ARCHIVED, L.SEQUENCE#, L.THREAD# ");
            sb.append("UNION ");
        }
        sb.append("SELECT A.NAME AS FILE_NAME, A.FIRST_CHANGE# FIRST_CHANGE, A.NEXT_CHANGE# NEXT_CHANGE, 'YES', ");
        sb.append("NULL, 'ARCHIVED', A.SEQUENCE# AS SEQ, A.DICTIONARY_BEGIN, A.DICTIONARY_END, A.THREAD# AS THREAD ");
        sb.append("FROM ").append(ARCHIVED_LOG_VIEW).append(" A, ").append(DATABASE_VIEW).append(" D ");
        sb.append("WHERE A.NAME IS NOT NULL ");
        sb.append("AND A.ARCHIVED = 'YES' ");
        sb.append("AND A.STATUS = 'A' ");
        sb.append("AND A.NEXT_CHANGE# > ").append(scn).append(" ");
        sb.append("AND A.DEST_ID IN (").append(localArchiveLogDestinationsOnlyQuery(archiveDestinationName, autonomousDatabaseMode)).append(") ");
        sb.append("AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE# ");
        sb.append("AND A.RESETLOGS_TIME = D.RESETLOGS_TIME ");

        if (!archiveLogRetention.isNegative() && !archiveLogRetention.isZero()) {
            sb.append("AND A.FIRST_TIME >= SYSDATE - (").append(archiveLogRetention.toHours()).append("/24) ");
        }

        return sb.append("ORDER BY 7").toString();
    }

    /**
     * Returns a SQL predicate clause that should be applied to any {@link #ARCHIVED_LOG_VIEW} queries
     * so that the results are filtered to only include the local destinations and not those that may
     * be generated by tools such as Oracle Data Guard.
     *
     * @param archiveDestinationName archive log destination to be used, may be {@code null} to auto-select
     * @param autonomousDatabaseMode true if running in Oracle Autonomous Database mode
     */
    private static String localArchiveLogDestinationsOnlyQuery(String archiveDestinationName, boolean autonomousDatabaseMode) {
        // In ADB, V$ARCHIVE_DEST_STATUS returns empty results since ADB manages archiving automatically.
        // When no specific destination is requested in ADB mode, include all DEST_IDs (1, 2, etc).
        if (autonomousDatabaseMode && Strings.isNullOrEmpty(archiveDestinationName)) {
            return String.format("SELECT DISTINCT DEST_ID FROM %s", ARCHIVED_LOG_VIEW);
        }

        final StringBuilder query = new StringBuilder(256);
        query.append("SELECT DEST_ID FROM ").append(ARCHIVE_DEST_STATUS_VIEW).append(" WHERE ");
        query.append("STATUS='VALID' AND TYPE='LOCAL' ");
        if (Strings.isNullOrEmpty(archiveDestinationName)) {
            query.append("AND ROWNUM=1");
        }
        else {
            query.append("AND UPPER(DEST_NAME)='").append(archiveDestinationName.toUpperCase()).append("'");
        }
        return query.toString();
    }

    // ***** LogMiner methods ***
    public static String getScnByTimeDeltaQuery(Scn scn, Duration duration) {
        if (scn == null) {
            return null;
        }

        return "select timestamp_to_scn(CAST(scn_to_timestamp(" + scn.toString() + ") as date)" +
                " - INTERVAL '" + duration.toMinutes() + "' MINUTE) from dual";
    }

}
