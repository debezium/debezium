/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.time.Duration;

import io.debezium.connector.oracle.OracleConnectorConfig;
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

    static String redoLogStatusQuery() {
        return String.format("SELECT F.MEMBER, R.STATUS FROM %s F, %s R WHERE F.GROUP# = R.GROUP# ORDER BY 2", LOGFILE_VIEW, LOG_VIEW);
    }

    static String switchHistoryQuery(String archiveDestinationName) {
        return String.format("SELECT 'TOTAL', COUNT(1) FROM %s WHERE FIRST_TIME > TRUNC(SYSDATE)" +
                " AND DEST_ID IN (" + localArchiveLogDestinationsOnlyQuery(archiveDestinationName) + ")",
                ARCHIVED_LOG_VIEW);
    }

    static String currentRedoNameQuery() {
        return String.format("SELECT F.MEMBER FROM %s LOG, %s F  WHERE LOG.GROUP#=F.GROUP# AND LOG.STATUS='CURRENT'", LOG_VIEW, LOGFILE_VIEW);
    }

    static String currentRedoLogSequenceQuery() {
        return String.format("SELECT SEQUENCE# FROM %s WHERE STATUS = 'CURRENT' ORDER BY SEQUENCE#", LOG_VIEW);
    }

    static String databaseSupplementalLoggingAllCheckQuery() {
        return String.format("SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_ALL FROM %s", DATABASE_VIEW);
    }

    static String databaseSupplementalLoggingMinCheckQuery() {
        return String.format("SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_MIN FROM %s", DATABASE_VIEW);
    }

    static String tableSupplementalLoggingCheckQuery() {
        return String.format("SELECT 'KEY', LOG_GROUP_TYPE FROM %s WHERE OWNER=? AND TABLE_NAME=?", ALL_LOG_GROUPS);
    }

    public static String oldestFirstChangeQuery(Duration archiveLogRetention, String archiveDestinationName) {
        final StringBuilder sb = new StringBuilder();
        sb.append("SELECT MIN(FIRST_CHANGE#) FROM (SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# ");
        sb.append("FROM ").append(LOG_VIEW).append(" ");
        sb.append("UNION SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# ");
        sb.append("FROM ").append(ARCHIVED_LOG_VIEW).append(" ");
        sb.append("WHERE DEST_ID IN (").append(localArchiveLogDestinationsOnlyQuery(archiveDestinationName)).append(") ");
        sb.append("AND STATUS='A'");

        if (!archiveLogRetention.isNegative() && !archiveLogRetention.isZero()) {
            sb.append(" AND FIRST_TIME >= SYSDATE - (").append(archiveLogRetention.toHours()).append("/24)");
        }

        return sb.append(")").toString();
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
            sb.append("FROM ").append(LOGFILE_VIEW).append(" F, ").append(LOG_VIEW).append(" L ");
            sb.append("LEFT JOIN ").append(ARCHIVED_LOG_VIEW).append(" A ");
            sb.append("ON A.FIRST_CHANGE# = L.FIRST_CHANGE# AND A.NEXT_CHANGE# = L.NEXT_CHANGE# ");
            sb.append("WHERE (A.STATUS <> 'A' OR A.FIRST_CHANGE# IS NULL) ");
            sb.append("AND L.STATUS != 'UNUSED' ");
            sb.append("AND F.GROUP# = L.GROUP# ");
            sb.append("GROUP BY F.GROUP#, L.FIRST_CHANGE#, L.NEXT_CHANGE#, L.STATUS, L.ARCHIVED, L.SEQUENCE#, L.THREAD# ");
            sb.append("UNION ");
        }
        sb.append("SELECT A.NAME AS FILE_NAME, A.FIRST_CHANGE# FIRST_CHANGE, A.NEXT_CHANGE# NEXT_CHANGE, 'YES', ");
        sb.append("NULL, 'ARCHIVED', A.SEQUENCE# AS SEQ, A.DICTIONARY_BEGIN, A.DICTIONARY_END, A.THREAD# AS THREAD ");
        sb.append("FROM ").append(ARCHIVED_LOG_VIEW).append(" A ");
        sb.append("WHERE A.NAME IS NOT NULL ");
        sb.append("AND A.ARCHIVED = 'YES' ");
        sb.append("AND A.STATUS = 'A' ");
        sb.append("AND A.NEXT_CHANGE# > ").append(scn).append(" ");
        sb.append("AND A.DEST_ID IN (").append(localArchiveLogDestinationsOnlyQuery(archiveDestinationName)).append(") ");

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
     */
    private static String localArchiveLogDestinationsOnlyQuery(String archiveDestinationName) {
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
    /**
     * This returns statement to build LogMiner view for online redo log files
     * @param startScn mine from
     * @param endScn mine till
     * @param strategy Log Mining strategy
     * @param continuousMining whether to use continuous mining
     * @return statement todo: handle corruption. STATUS (Double) — value of 0 indicates it is executable
     */
    static String startLogMinerStatement(Scn startScn, Scn endScn, OracleConnectorConfig.LogMiningStrategy strategy, boolean continuousMining) {
        String miningStrategy;
        if (strategy.equals(OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO)) {
            miningStrategy = "DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING ";
        }
        else {
            miningStrategy = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG ";
        }
        if (continuousMining) {
            miningStrategy += " + DBMS_LOGMNR.CONTINUOUS_MINE ";
        }
        return "BEGIN sys.dbms_logmnr.start_logmnr(" +
                "startScn => '" + startScn + "', " +
                "endScn => '" + endScn + "', " +
                "OPTIONS => " + miningStrategy +
                " + DBMS_LOGMNR.NO_ROWID_IN_STMT);" +
                "END;";
    }

    static String addLogFileStatement(String option, String fileName) {
        return "BEGIN sys.dbms_logmnr.add_logfile(LOGFILENAME => '" + fileName + "', OPTIONS => " + option + ");END;";
    }

    static String deleteLogFileStatement(String fileName) {
        return "BEGIN SYS.DBMS_LOGMNR.REMOVE_LOGFILE(LOGFILENAME => '" + fileName + "');END;";
    }

    public static String getScnByTimeDeltaQuery(Scn scn, Duration duration) {
        if (scn == null) {
            return null;
        }

        return "select timestamp_to_scn(CAST(scn_to_timestamp(" + scn.toString() + ") as date)" +
                " - INTERVAL '" + duration.toMinutes() + "' MINUTE) from dual";
    }

}
