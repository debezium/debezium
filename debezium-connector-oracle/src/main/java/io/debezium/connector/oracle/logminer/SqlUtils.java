/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.relational.TableId;

/**
 * This utility class contains SQL statements to configure, manage and query Oracle LogMiner
 */
class SqlUtils {

    private static final String LOGMNR_CONTENTS_VIEW = "V$LOGMNR_CONTENTS";
    private static final String LOGMNR_AUDIT_TABLE = "LOG_MINING_AUDIT";

    static final String BUILD_DICTIONARY = "BEGIN DBMS_LOGMNR_D.BUILD (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;";
    static final String CURRENT_SCN = "SELECT CURRENT_SCN FROM V$DATABASE";
    static final String CURRENT_TIMESTAMP = "select current_timestamp from dual";
    static final String END_LOGMNR = "BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;";
    static final String OLDEST_FIRST_CHANGE = "SELECT MIN(FIRST_CHANGE#) FROM V$LOG";
    static final String ALL_ONLINE_LOGS = "SELECT MIN(F.MEMBER) AS FILE_NAME, L.NEXT_CHANGE# AS NEXT_CHANGE, F.GROUP# " +
            " FROM V$LOG L, V$LOGFILE F " +
            " WHERE F.GROUP# = L.GROUP# AND L.NEXT_CHANGE# > 0 " +
            " GROUP BY F.GROUP#, L.NEXT_CHANGE# ORDER BY 3";

    static final String REDO_LOGS_STATUS = "SELECT F.MEMBER, R.STATUS FROM V$LOGFILE F, V$LOG R WHERE F.GROUP# = R.GROUP# ORDER BY 2";
    static final String SWITCH_HISTORY_TOTAL_COUNT = "select 'total', count(1) from v$archived_log where first_time > trunc(sysdate)" +
            " and dest_id = (select dest_id from V$ARCHIVE_DEST_STATUS where status='VALID' and type='LOCAL')";
    static final String CURRENT_REDO_LOG_NAME = "select f.member from v$log log, v$logfile f  where log.group#=f.group# and log.status='CURRENT'";
    static final String AUDIT_TABLE_EXISTS = "SELECT '1' AS ONE FROM USER_TABLES WHERE TABLE_NAME = '" + LOGMNR_AUDIT_TABLE + "'";
    static final String AUDIT_TABLE_RECORD_EXISTS = "SELECT '1' AS ONE FROM " + LOGMNR_AUDIT_TABLE;
    static final String CREATE_AUDIT_TABLE = "CREATE TABLE " + LOGMNR_AUDIT_TABLE + "(LAST_SCN NUMBER(19,0))";
    static final String INSERT_AUDIT_TABLE = "INSERT INTO " + LOGMNR_AUDIT_TABLE + " VALUES(0)";
    static final String UPDATE_AUDIT_TABLE = "UPDATE " + LOGMNR_AUDIT_TABLE + " SET LAST_SCN =";

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlUtils.class);

    // todo handle INVALID file member (report somehow and continue to work with valid file), handle adding multiplexed files,
    // todo SELECT name, value FROM v$sysstat WHERE name = 'redo wastage';
    // todo SELECT GROUP#, STATUS, MEMBER FROM V$LOGFILE WHERE STATUS='INVALID'; (drop and recreate? or do it manually?)
    // todo SELECT BLOCKSIZE FROM V$LOG;

    static final String NLS_SESSION_PARAMETERS = "ALTER SESSION SET "
            + "  NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
            + "  NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'"
            + "  NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'"
            + "  NLS_NUMERIC_CHARACTERS = '.,'";

    /**
     * This returns statement to build log miner view for online redo log files
     * @param startScn mine from
     * @param endScn mine till
     * @param strategy Log Mining strategy
     * @return statement todo: handle corruption. STATUS (Double) — value of 0 indicates it is executable
     */
    static String getStartLogMinerStatement(Long startScn, Long endScn, OracleConnectorConfig.LogMiningStrategy strategy, boolean isContinuousMining) {
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
    static String queryLogMinerContents(String schemaName, String logMinerUser, OracleDatabaseSchema schema) {
        List<String> whiteListTableNames = schema.tableIds().stream().map(TableId::table).collect(Collectors.toList());

        String sorting = "ORDER BY SCN";
        return "SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME " +
                " FROM " + LOGMNR_CONTENTS_VIEW + " WHERE  OPERATION_CODE in (1,2,3,5) " + // 5 - DDL
                " AND SEG_OWNER = '" + schemaName.toUpperCase() + "' " +
                buildTableInPredicate(whiteListTableNames) +
                " AND SCN >= ? AND SCN < ? " +
                " OR (OPERATION_CODE IN (5,7,34,36) AND USERNAME NOT IN ('SYS','SYSTEM','" + logMinerUser.toUpperCase() + "'))" + sorting; // todo username = schemaName?
    }

    static String getAddLogFileStatement(String option, String fileName) {
        return "BEGIN sys.dbms_logmnr.add_logfile(LOGFILENAME => '" + fileName + "', OPTIONS => " + option + ");END;";
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
