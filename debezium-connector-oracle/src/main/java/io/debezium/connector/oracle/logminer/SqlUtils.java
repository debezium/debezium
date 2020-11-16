/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.io.IOException;
import java.sql.SQLRecoverableException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.relational.TableId;

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
    private static final String LOGMNR_CONTENTS_VIEW = "V$LOGMNR_CONTENTS";

    // log miner statements
    static final String BUILD_DICTIONARY = "BEGIN DBMS_LOGMNR_D.BUILD (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;";
    static final String CURRENT_TIMESTAMP = "SELECT CURRENT_TIMESTAMP FROM DUAL";
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
    // todo: this table shifted from LOG_MINING_AUDIT to LOG_MINING_FLUSH.
    // since this is only used during streaming to flush redo log buffer, accept this change?
    static final String LOGMNR_FLUSH_TABLE = "LOG_MINING_FLUSH";
    static final String FLUSH_TABLE_NOT_EMPTY = "SELECT '1' AS ONE FROM " + LOGMNR_FLUSH_TABLE;
    static final String CREATE_FLUSH_TABLE = "CREATE TABLE " + LOGMNR_FLUSH_TABLE + "(LAST_SCN NUMBER(19,0)) TABLESPACE LOGMINER_TBS";
    static final String INSERT_FLUSH_TABLE = "INSERT INTO " + LOGMNR_FLUSH_TABLE + " VALUES(0)";
    static final String UPDATE_FLUSH_TABLE = "UPDATE " + LOGMNR_FLUSH_TABLE + " SET LAST_SCN =";

    // history recording statements
    static final String LOGMNR_HISTORY_TEMP_TABLE = "LOG_MINING_TEMP";
    static final String LOGMNR_HISTORY_TABLE_PREFIX = "LM_HIST_";
    private static final String LOGMNR_HISTORY_SEQUENCE = "LOG_MINING_HIST_SEQ";
    static final String CREATE_LOGMINING_HISTORY_SEQUENCE = "CREATE SEQUENCE " + LOGMNR_HISTORY_SEQUENCE + " ORDER CACHE 10000";
    static final String LOGMINING_HISTORY_SEQUENCE_EXISTS = "SELECT '1' AS ONE FROM USER_SEQUENCES WHERE SEQUENCE_NAME = '" + LOGMNR_HISTORY_SEQUENCE + "'";
    static final String INSERT_INTO_TEMP_HISTORY_TABLE_STMT = "INSERT /*+ APPEND */ INTO " + LOGMNR_HISTORY_TEMP_TABLE +
            " values(" + LOGMNR_HISTORY_SEQUENCE + ".nextVal, ?, ?, ?, ?, ?, ?, ?, ?)";

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

    static String switchHistoryQuery() {
        return String.format("SELECT 'TOTAL', COUNT(1) FROM %s WHERE FIRST_TIME > TRUNC(SYSDATE)" +
                " AND DEST_ID IN (SELECT DEST_ID FROM %s WHERE STATUS='VALID' AND TYPE='LOCAL')",
                ARCHIVED_LOG_VIEW,
                ARCHIVE_DEST_STATUS_VIEW);
    }

    static String currentRedoNameQuery() {
        return String.format("SELECT F.MEMBER FROM %s LOG, %s F  WHERE LOG.GROUP#=F.GROUP# AND LOG.STATUS='CURRENT'", LOG_VIEW, LOGFILE_VIEW);
    }

    static String supplementalLoggingCheckQuery() {
        return String.format("SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_ALL FROM %s", DATABASE_VIEW);
    }

    static String currentScnQuery() {
        return String.format("SELECT CURRENT_SCN FROM %s", DATABASE_VIEW);
    }

    static String oldestFirstChangeQuery() {
        return String.format("SELECT MIN(FIRST_CHANGE#) FROM %s", LOG_VIEW);
    }

    public static String allOnlineLogsQuery() {
        return String.format("SELECT MIN(F.MEMBER) AS FILE_NAME, L.NEXT_CHANGE# AS NEXT_CHANGE, F.GROUP# " +
                " FROM %s L, %s F " +
                " WHERE F.GROUP# = L.GROUP# AND L.NEXT_CHANGE# > 0 " +
                " GROUP BY F.GROUP#, L.NEXT_CHANGE# ORDER BY 3", LOG_VIEW, LOGFILE_VIEW);
    }

    /**
     * Hardcoded retention policy = SYSDATE -1
     *
     * @param scn oldest scn to search for
     * @return query
     */
    public static String oneDayArchivedLogsQuery(Long scn) {
        return String.format("SELECT NAME AS FILE_NAME, NEXT_CHANGE# AS NEXT_CHANGE FROM %s " +
                " WHERE NAME IS NOT NULL AND FIRST_TIME >= SYSDATE - 1 AND ARCHIVED = 'YES' " +
                " AND STATUS = 'A' AND NEXT_CHANGE# > %s ORDER BY 2", ARCHIVED_LOG_VIEW, scn);
    }

    // ***** log miner methods ***
    /**
     * This returns statement to build log miner view for online redo log files
     * @param startScn mine from
     * @param endScn mine till
     * @param strategy Log Mining strategy
     * @return statement todo: handle corruption. STATUS (Double) — value of 0 indicates it is executable
     */
    static String startLogMinerStatement(Long startScn, Long endScn, OracleConnectorConfig.LogMiningStrategy strategy, boolean isContinuousMining) {
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

    /**
     * This is the query from the log miner view to get changes. Columns of the view we using are:
     * NOTE. Currently we do not capture changes from other schemas
     * SCN - The SCN at which a change was made
     * COMMIT_SCN - The SCN at which a change was committed
     * USERNAME - Name of the user who executed the transaction
     * SQL_REDO Reconstructed SQL statement that is equivalent to the original SQL statement that made the change
     * OPERATION_CODE - Number of the operation code.
     * TABLE_NAME - Name of the modified table
     * TIMESTAMP - Timestamp when the database change was made
     *
     * @param schemaName user name
     * @param logMinerUser log mining session user name
     * @param schema schema
     * @return the query
     */
    static String logMinerContentsQuery(String schemaName, String logMinerUser, OracleDatabaseSchema schema) {
        List<String> whiteListTableNames = schema.tableIds().stream().map(TableId::table).collect(Collectors.toList());

        String sorting = "ORDER BY SCN";
        // todo: add ROW_ID, SESSION#, SERIAL#, RS_ID, and SSN
        return "SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME " +
                " FROM " + LOGMNR_CONTENTS_VIEW + " WHERE  OPERATION_CODE in (1,2,3,5) " + // 5 - DDL
                " AND SEG_OWNER = '" + schemaName.toUpperCase() + "' " +
                buildTableInPredicate(whiteListTableNames) +
                " AND SCN >= ? AND SCN < ? " +
                // todo: check with Andrey why they haven't incorporated operation_code 5 here yet?
                " OR (OPERATION_CODE IN (5,7,34,36) AND USERNAME NOT IN ('SYS','SYSTEM','" + logMinerUser.toUpperCase() + "'))" + sorting; // todo username = schemaName?
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

    static String getHistoryTableNamesQuery() {
        return "SELECT TABLE_NAME, '1' FROM USER_TABLES WHERE TABLE_NAME LIKE '" + LOGMNR_HISTORY_TABLE_PREFIX + "%'";
    }

    static String dropHistoryTableStatement(String tableName) {
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
                ") nologging tablespace LOGMINER_TBS";
    }

    static String truncateTableStatement(String tableName) {
        return "TRUNCATE TABLE " + tableName;
    }

    static String bulkHistoryInsertStmt(String currentHistoryTableName) {
        return "INSERT  /*+ APPEND */ INTO " + currentHistoryTableName + " SELECT * FROM " + LOGMNR_HISTORY_TEMP_TABLE;
    }

    /**
     * This method return query which converts given SCN in days and deduct from the current day
     */
    public static String diffInDaysQuery(Long scn) {
        if (scn == null) {
            return null;
        }
        return "select sysdate - CAST(scn_to_timestamp(" + scn + ") as date) from dual";
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

    public static String buildHistoryTableName(LocalDateTime now) {
        int year = now.getYear();
        int month = now.getMonthValue();
        int day = now.getDayOfMonth();
        int hour = now.getHour();
        int minute = now.getMinute();
        return SqlUtils.LOGMNR_HISTORY_TABLE_PREFIX + year + "_" + month + "_" + day + "_" + hour + "_" + minute;
    }

    public static long parseRetentionFromName(String historyTableName) {
        String[] tokens = historyTableName.split("_");
        if (tokens.length != 7) {
            return 0;
        }
        LocalDateTime recorded = LocalDateTime.of(
                Integer.parseInt(tokens[2]), // year
                Integer.parseInt(tokens[3]), // month
                Integer.parseInt(tokens[4]), // days
                Integer.parseInt(tokens[5]), // hours
                Integer.parseInt(tokens[6])); // minutes
        return Duration.between(recorded, LocalDateTime.now()).toHours();
    }

    /**
     * This method builds table_name IN predicate, filtering out non whitelisted tables from Log Mining.
     * It limits joining condition over 1000 tables, Oracle will throw exception in such predicate.
     * @param tables white listed table names
     * @return IN predicate or empty string if number of whitelisted tables exceeds 1000
     */
    private static String buildTableInPredicate(List<String> tables) {
        if (tables.size() == 0 || tables.size() > 1000) {
            LOGGER.warn(" Cannot apply {} whitelisted tables condition", tables.size());
            return "";
        }

        StringJoiner tableNames = new StringJoiner(",");
        tables.forEach(table -> tableNames.add("'" + table + "'"));
        return " AND table_name IN (" + tableNames + ") ";
    }
}
