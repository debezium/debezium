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
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

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
    private static final String LOGMNR_CONTENTS_VIEW = "V$LOGMNR_CONTENTS";
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

    static String switchHistoryQuery() {
        return String.format("SELECT 'TOTAL', COUNT(1) FROM %s WHERE FIRST_TIME > TRUNC(SYSDATE)" +
                " AND DEST_ID IN (" + localArchiveLogDestinationsOnlyQuery() + ")",
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

    static String oldestFirstChangeQuery(Duration archiveLogRetention) {
        final StringBuilder sb = new StringBuilder();
        sb.append("SELECT MIN(FIRST_CHANGE#) FROM (SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# ");
        sb.append("FROM ").append(LOG_VIEW).append(" ");
        sb.append("UNION SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# ");
        sb.append("FROM ").append(ARCHIVED_LOG_VIEW).append(" ");
        sb.append("WHERE DEST_ID IN (").append(localArchiveLogDestinationsOnlyQuery()).append(") ");
        sb.append("AND STATUS='A'");

        if (!archiveLogRetention.isNegative() && !archiveLogRetention.isZero()) {
            sb.append("AND FIRST_TIME >= SYSDATE - (").append(archiveLogRetention.toHours()).append("/24)");
        }

        return sb.append(")").toString();
    }

    public static String allOnlineLogsQuery() {
        return String.format("SELECT MIN(F.MEMBER) AS FILE_NAME, L.NEXT_CHANGE# AS NEXT_CHANGE, F.GROUP#, L.FIRST_CHANGE# AS FIRST_CHANGE, L.STATUS " +
                " FROM %s L, %s F " +
                " WHERE F.GROUP# = L.GROUP# AND L.NEXT_CHANGE# > 0 " +
                " GROUP BY F.GROUP#, L.NEXT_CHANGE#, L.FIRST_CHANGE#, L.STATUS ORDER BY 3", LOG_VIEW, LOGFILE_VIEW);
    }

    /**
     * Obtain the query to be used to fetch archive logs.
     *
     * @param scn oldest scn to search for
     * @param archiveLogRetention duration archive logs will be mined
     * @return query
     */
    public static String archiveLogsQuery(Scn scn, Duration archiveLogRetention) {
        final StringBuilder sb = new StringBuilder();
        sb.append("SELECT NAME AS FILE_NAME, NEXT_CHANGE# AS NEXT_CHANGE, FIRST_CHANGE# AS FIRST_CHANGE ");
        sb.append("FROM ").append(ARCHIVED_LOG_VIEW).append(" ");
        sb.append("WHERE NAME IS NOT NULL ");
        sb.append("AND ARCHIVED = 'YES' ");
        sb.append("AND STATUS = 'A' ");
        sb.append("AND NEXT_CHANGE# > ").append(scn).append(" ");
        sb.append("AND DEST_ID IN (").append(localArchiveLogDestinationsOnlyQuery()).append(") ");

        if (!archiveLogRetention.isNegative() && !archiveLogRetention.isZero()) {
            sb.append("AND FIRST_TIME >= SYSDATE - (").append(archiveLogRetention.toHours()).append("/24) ");
        }

        return sb.append("ORDER BY 2").toString();
    }

    /**
     * Returns a SQL predicate clause that should be applied to any {@link #ARCHIVED_LOG_VIEW} queries
     * so that the results are filtered to only include the local destinations and not those that may
     * be generated by tools such as Oracle Data Guard.
     */
    private static String localArchiveLogDestinationsOnlyQuery() {
        return String.format("SELECT DEST_ID FROM %s WHERE STATUS='VALID' AND TYPE='LOCAL' AND ROWNUM=1", ARCHIVE_DEST_STATUS_VIEW);
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

    // ***** LogMiner methods ***

    /**
     * This returns statement to build LogMiner view for online redo log files
     *
     * @param startScn mine from
     * @param endScn   mine till
     * @param strategy Log Mining strategy
     * @return statement todo: handle corruption. STATUS (Double) — value of 0 indicates it is executable
     */
    static String startLogMinerStatement(Scn startScn, Scn endScn, OracleConnectorConfig.LogMiningStrategy strategy, boolean isContinuousMining,
                                         boolean committedDataOnly, String extOptions) {
        final String miningStrategy = getMiningStrategy(strategy, isContinuousMining, committedDataOnly, extOptions);
        return "BEGIN sys.dbms_logmnr.start_logmnr(" +
                "startScn => '" + startScn + "', " +
                "endScn => '" + endScn + "', " +
                "OPTIONS => " + miningStrategy +
                " + DBMS_LOGMNR.NO_ROWID_IN_STMT);" +
                "END;";
    }

    private static String getMiningStrategy(OracleConnectorConfig.LogMiningStrategy strategy, boolean isContinuousMining, boolean committedDataOnly, String extOptions) {
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
        if (committedDataOnly) {
            miningStrategy += " + DBMS_LOGMNR.COMMITTED_DATA_ONLY ";
        }
        if (extOptions != null && (!extOptions.isEmpty())) {
            miningStrategy += extOptions.toUpperCase();
        }
        return miningStrategy;
    }

    /**
     * This is the query from the LogMiner view to get changes.
     *
     * The query uses the following columns from the view:
     * <pre>
     * SCN - The SCN at which a change was made
     * SQL_REDO Reconstructed SQL statement that is equivalent to the original SQL statement that made the change
     * OPERATION_CODE - Number of the operation code
     * TIMESTAMP - Timestamp when the database change was made
     * XID - Transaction Identifier
     * CSF - Continuation SQL flag, identifies rows that should be processed together as a single row (0=no,1=yes)
     * TABLE_NAME - Name of the modified table
     * SEG_OWNER - Schema/Tablespace name
     * OPERATION - Database operation type
     * USERNAME - Name of the user who executed the transaction
     * </pre>
     *
     * @param connectorConfig the connector configuration
     * @param logMinerUser log mining session user name
     * @return the query
     */
    static String logMinerContentsQuery(OracleConnectorConfig connectorConfig, String logMinerUser) {
        StringBuilder query = new StringBuilder();
        query.append("SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME, ROW_ID, ROLLBACK ");
        query.append("FROM ").append(LOGMNR_CONTENTS_VIEW).append(" ");
        query.append("WHERE ");
        query.append("SCN > ? AND SCN <= ? ");
        query.append("AND (");
        // MISSING_SCN/DDL only when not performed by excluded users
        query.append("(OPERATION_CODE IN (5,34) AND USERNAME NOT IN (").append(getExcludedUsers(logMinerUser)).append(")) ");
        // COMMIT/ROLLBACK
        query.append("OR (OPERATION_CODE IN (7,36)) ");
        // INSERT/UPDATE/DELETE
        query.append("OR ");
        query.append("(OPERATION_CODE IN (1,2,3) ");
        query.append("AND TABLE_NAME != '").append(LOGMNR_FLUSH_TABLE).append("' ");

        // There are some common schemas that we automatically ignore when building the filter predicates
        // and we pull that same list of schemas in here and apply those exclusions in the generated SQL.
        if (!OracleConnectorConfig.EXCLUDED_SCHEMAS.isEmpty()) {
            query.append("AND SEG_OWNER NOT IN (");
            for (Iterator<String> i = OracleConnectorConfig.EXCLUDED_SCHEMAS.iterator(); i.hasNext();) {
                String excludedSchema = i.next();
                query.append("'").append(excludedSchema.toUpperCase()).append("'");
                if (i.hasNext()) {
                    query.append(",");
                }
            }
            query.append(") ");
        }

        String schemaPredicate = buildSchemaPredicate(connectorConfig);
        if (!Strings.isNullOrEmpty(schemaPredicate)) {
            query.append("AND ").append(schemaPredicate).append(" ");
        }

        String tablePredicate = buildTablePredicate(connectorConfig);
        if (!Strings.isNullOrEmpty(tablePredicate)) {
            query.append("AND ").append(tablePredicate).append(" ");
        }

        query.append("))");

        return query.toString();
    }

    private static String getExcludedUsers(String logMinerUser) {
        return "'SYS','SYSTEM','" + logMinerUser.toUpperCase() + "'";
    }

    private static String buildSchemaPredicate(OracleConnectorConfig connectorConfig) {
        StringBuilder predicate = new StringBuilder();
        if (Strings.isNullOrEmpty(connectorConfig.schemaIncludeList())) {
            if (!Strings.isNullOrEmpty(connectorConfig.schemaExcludeList())) {
                List<Pattern> patterns = Strings.listOfRegex(connectorConfig.schemaExcludeList(), 0);
                predicate.append("(").append(listOfPatternsToSql(patterns, "SEG_OWNER", true)).append(")");
            }
        }
        else {
            List<Pattern> patterns = Strings.listOfRegex(connectorConfig.schemaIncludeList(), 0);
            predicate.append("(").append(listOfPatternsToSql(patterns, "SEG_OWNER", false)).append(")");
        }
        return predicate.toString();
    }

    private static String buildTablePredicate(OracleConnectorConfig connectorConfig) {
        StringBuilder predicate = new StringBuilder();
        if (Strings.isNullOrEmpty(connectorConfig.tableIncludeList())) {
            if (!Strings.isNullOrEmpty(connectorConfig.tableExcludeList())) {
                List<Pattern> patterns = Strings.listOfRegex(connectorConfig.tableExcludeList(), 0);
                predicate.append("(").append(listOfPatternsToSql(patterns, "SEG_OWNER || '.' || TABLE_NAME", true)).append(")");
            }
        }
        else {
            List<Pattern> patterns = Strings.listOfRegex(connectorConfig.tableIncludeList(), 0);
            predicate.append("(").append(listOfPatternsToSql(patterns, "SEG_OWNER || '.' || TABLE_NAME", false)).append(")");
        }
        return predicate.toString();
    }

    private static String listOfPatternsToSql(List<Pattern> patterns, String columnName, boolean applyNot) {
        StringBuilder predicate = new StringBuilder();
        for (Iterator<Pattern> i = patterns.iterator(); i.hasNext();) {
            Pattern pattern = i.next();
            if (applyNot) {
                predicate.append("NOT ");
            }
            // NOTE: The REGEXP_LIKE operator was added in Oracle 10g (10.1.0.0.0)
            final String text = resolveRegExpLikePattern(pattern);
            predicate.append("REGEXP_LIKE(").append(columnName).append(",'").append(text).append("','i')");
            if (i.hasNext()) {
                // Exclude lists imply combining them via AND, Include lists imply combining them via OR?
                predicate.append(applyNot ? " AND " : " OR ");
            }
        }
        return predicate.toString();
    }

    private static String resolveRegExpLikePattern(Pattern pattern) {
        // The REGEXP_LIKE operator acts identical to LIKE in that it automatically prepends/appends "%".
        // We need to resolve our matches to be explicit with "^" and "$" if they don't already exist so
        // that the LIKE aspect of the match doesn't mistakenly filter "DEBEZIUM2" when using "DEBEZIUM".
        String text = pattern.pattern();
        if (!text.startsWith("^")) {
            text = "^" + text;
        }
        if (!text.endsWith("$")) {
            text += "$";
        }
        return text;
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
}
