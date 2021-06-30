/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.io.IOException;
import java.sql.SQLRecoverableException;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;

import oracle.net.ns.NetException;

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

    // LogMiner statements
    static final String BUILD_DICTIONARY = "BEGIN DBMS_LOGMNR_D.BUILD (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;";
    static final String SELECT_SYSTIMESTAMP = "SELECT SYSTIMESTAMP FROM DUAL";
    static final String END_LOGMNR = "BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;";

    /**
     * Querying V$LOGMNR_LOGS
     * After a successful call to DBMS_LOGMNR.START_LOGMNR, the STATUS column of the V$LOGMNR_LOGS view contains one of the following values:
     * 0
     * Indicates that the redo log file will be processed during a query of the V$LOGMNR_CONTENTS view.
     * 1
     * Indicates that this will be the first redo log file to be processed by LogMiner during a select operation against the V$LOGMNR_CONTENTS view.
     * 2
     * Indicates that the redo log file has been pruned and therefore will not be processed by LogMiner during a query of the V$LOGMNR_CONTENTS view.
     * It has been pruned because it is not needed to satisfy your requested time or SCN range.
     * 4
     * Indicates that a redo log file (based on sequence number) is missing from the LogMiner redo log file list.
     */
    static final String FILES_FOR_MINING = "SELECT FILENAME AS NAME FROM V$LOGMNR_LOGS";

    // log writer flush statements
    public static final String LOGMNR_FLUSH_TABLE = "LOG_MINING_FLUSH";
    static final String FLUSH_TABLE_NOT_EMPTY = "SELECT '1' AS ONE FROM " + LOGMNR_FLUSH_TABLE;
    static final String CREATE_FLUSH_TABLE = "CREATE TABLE " + LOGMNR_FLUSH_TABLE + "(LAST_SCN NUMBER(19,0))";
    static final String INSERT_FLUSH_TABLE = "INSERT INTO " + LOGMNR_FLUSH_TABLE + " VALUES(0)";
    static final String UPDATE_FLUSH_TABLE = "UPDATE " + LOGMNR_FLUSH_TABLE + " SET LAST_SCN =";

    static final String NLS_SESSION_PARAMETERS = "ALTER SESSION SET "
            + "  NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
            + "  NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'"
            + "  NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'"
            + "  NLS_NUMERIC_CHARACTERS = '.,'";

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlUtils.class);

    static void setRac(boolean isRac) {
        if (isRac) {
            // todo : enforce continious_mine=false?
        }
    }

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
        return String.format("SELECT SEQUENCE# FROM %s WHERE STATUS = 'CURRENT'", LOG_VIEW);
    }

    static String databaseSupplementalLoggingAllCheckQuery() {
        return String.format("SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_ALL FROM %s", DATABASE_VIEW);
    }

    static String databaseSupplementalLoggingMinCheckQuery() {
        return String.format("SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_MIN FROM %s", DATABASE_VIEW);
    }

    static String tableSupplementalLoggingCheckQuery(TableId tableId) {
        return String.format("SELECT 'KEY', LOG_GROUP_TYPE FROM %s WHERE OWNER = '%s' AND TABLE_NAME = '%s'", ALL_LOG_GROUPS, tableId.schema(), tableId.table());
    }

    static String currentScnQuery() {
        return String.format("SELECT CURRENT_SCN FROM %s", DATABASE_VIEW);
    }

    static String oldestFirstChangeQuery(Duration archiveLogRetention, String archiveDestinationName) {
        final StringBuilder sb = new StringBuilder();
        sb.append("SELECT MIN(FIRST_CHANGE#) FROM (SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# ");
        sb.append("FROM ").append(LOG_VIEW).append(" ");
        sb.append("UNION SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# ");
        sb.append("FROM ").append(ARCHIVED_LOG_VIEW).append(" ");
        sb.append("WHERE DEST_ID IN (").append(localArchiveLogDestinationsOnlyQuery(archiveDestinationName)).append(") ");
        sb.append("AND STATUS='A'");

        if (!archiveLogRetention.isNegative() && !archiveLogRetention.isZero()) {
            sb.append("AND FIRST_TIME >= SYSDATE - (").append(archiveLogRetention.toHours()).append("/24)");
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
        // L.STATUS, 'ONLINE' AS TYPE, L.SEQUENCE# AS SEQ, 'NO' AS DICT_START, 'NO AS DICT_END
        // FROM V$LOGFILE F, V$LOG L
        // LEFT JOIN V$ARCHIVED_LOG A
        // ON A.FIRST_CHANGE# = L.FIRST_CHANGE# AND A.NEXT_CHANGE# = L.NEXT_CHANGE#
        // WHERE A.FIRST_CHANGE# IS NULL
        // AND F.GROUP# = L.GROUP#
        // GROUP BY F.GROUP#, L.FIRST_CHANGE#, L.NEXT_CHANGE#, L.STATUS, L.ARCHIVED, L.SEQUENCE#
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
        // NULL, 'ARCHIVED', A.SEQUENCE# AS SEQ, A.DICTIONARY_BEGIN, A.DICTIONARY_END
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
            sb.append("L.STATUS, 'ONLINE' AS TYPE, L.SEQUENCE# AS SEQ, 'NO' AS DICT_START, 'NO' AS DICT_END ");
            sb.append("FROM ").append(LOGFILE_VIEW).append(" F, ").append(LOG_VIEW).append(" L ");
            sb.append("LEFT JOIN ").append(ARCHIVED_LOG_VIEW).append(" A ");
            sb.append("ON A.FIRST_CHANGE# = L.FIRST_CHANGE# AND A.NEXT_CHANGE# = L.NEXT_CHANGE# ");
            sb.append("WHERE A.FIRST_CHANGE# IS NULL ");
            sb.append("AND F.GROUP# = L.GROUP# ");
            sb.append("GROUP BY F.GROUP#, L.FIRST_CHANGE#, L.NEXT_CHANGE#, L.STATUS, L.ARCHIVED, L.SEQUENCE# ");
            sb.append("UNION ");
        }
        sb.append("SELECT A.NAME AS FILE_NAME, A.FIRST_CHANGE# FIRST_CHANGE, A.NEXT_CHANGE# NEXT_CHANGE, 'YES', ");
        sb.append("NULL, 'ARCHIVED', A.SEQUENCE# AS SEQ, A.DICTIONARY_BEGIN, A.DICTIONARY_END ");
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
     * @return statement todo: handle corruption. STATUS (Double) — value of 0 indicates it is executable
     */
    static String startLogMinerStatement(Scn startScn, Scn endScn, OracleConnectorConfig.LogMiningStrategy strategy, boolean isContinuousMining) {
        String miningStrategy;
        if (strategy.equals(OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO)) {
            miningStrategy = "DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING ";
        }
        else {
            miningStrategy = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG ";
        }
        if (isContinuousMining) {
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

    static String tableExistsQuery(String tableName) {
        return "SELECT '1' AS ONE FROM USER_TABLES WHERE TABLE_NAME = '" + tableName + "'";
    }

    static String dropTableStatement(String tableName) {
        return "DROP TABLE " + tableName.toUpperCase() + " PURGE";
    }

    // no constraints, no indexes, minimal info
    static String logMiningHistoryDdl(String tableName) {
        return "create  TABLE " + tableName + "(" +
                "row_sequence NUMBER(19,0), " +
                "captured_scn NUMBER(19,0), " +
                "table_name VARCHAR2(30 CHAR), " +
                "seg_owner VARCHAR2(30 CHAR), " +
                "operation_code NUMBER(19,0), " +
                "change_time TIMESTAMP(6), " +
                // "row_id VARCHAR2(20 CHAR)," +
                // "session_num NUMBER(19,0)," +
                // "serial_num NUMBER(19,0)," +
                "transaction_id VARCHAR2(50 CHAR), " +
                // "rs_id VARCHAR2(34 CHAR)," +
                // "ssn NUMBER(19,0)," +
                "csf NUMBER(19,0), " +
                "redo_sql VARCHAR2(4000 CHAR)" +
                // "capture_time TIMESTAMP(6)" +
                ") nologging";
    }

    static String truncateTableStatement(String tableName) {
        return "TRUNCATE TABLE " + tableName;
    }

    /**
     * This method return query which converts given SCN in days and deduct from the current day
     */
    public static String diffInDaysQuery(Scn scn) {
        if (scn == null) {
            return null;
        }
        return "select sysdate - CAST(scn_to_timestamp(" + scn.toString() + ") as date) from dual";
    }

    public static boolean connectionProblem(Throwable e) {
        if (e instanceof IOException) {
            return true;
        }
        Throwable cause = e.getCause();
        if (cause != null) {
            if (cause.getCause() != null && cause.getCause() instanceof NetException) {
                return true;
            }
        }
        if (e instanceof SQLRecoverableException) {
            return true;
        }
        if (e.getMessage() == null) {
            return false;
        }
        return e.getMessage().startsWith("ORA-03135") || // connection lost contact
                e.getMessage().startsWith("ORA-12543") || // TNS:destination host unreachable
                e.getMessage().startsWith("ORA-00604") || // error occurred at recursive SQL level 1
                e.getMessage().startsWith("ORA-01089") || // Oracle immediate shutdown in progress
                e.getMessage().startsWith("ORA-00600") || // Oracle internal error on the RAC node shutdown could happen
                e.getMessage().toUpperCase().contains("CONNECTION IS CLOSED") ||
                e.getMessage().toUpperCase().startsWith("NO MORE DATA TO READ FROM SOCKET");
    }
}
