/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;
import io.debezium.util.HexConverter;

/**
 * A utility class to map LogMiner content resultSet values.
 * This class gracefully logs errors, loosing an entry is not critical.
 * The loss will be logged
 */
public class RowMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(RowMapper.class);

    // operations
    public static final int INSERT = 1;
    public static final int DELETE = 2;
    public static final int UPDATE = 3;
    public static final int DDL = 5;
    public static final int START = 6;
    public static final int COMMIT = 7;
    public static final int SELECT_LOB_LOCATOR = 9;
    public static final int LOB_WRITE = 10;
    public static final int LOB_ERASE = 29;
    public static final int MISSING_SCN = 34;
    public static final int ROLLBACK = 36;

    // columns mapped by index based on query in SqlUtils#logMinerContentsQuery
    private static final int SCN = 1;
    private static final int SQL_REDO = 2;
    private static final int OPERATION_CODE = 3;
    private static final int CHANGE_TIME = 4;
    private static final int TX_ID = 5;
    private static final int CSF = 6;
    private static final int TABLE_NAME = 7;
    private static final int SEG_OWNER = 8;
    private static final int OPERATION = 9;
    private static final int USERNAME = 10;
    private static final int ROW_ID = 11;
    private static final int ROLLBACK_FLAG = 12;
    private static final int RS_ID = 13;

    private static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC));

    // todo: add these for recording
    // private static final int SESSION_NUMBER = 10;
    // private static final int SERIAL_NUMBER = 11;

    public static String getOperation(ResultSet rs) throws SQLException {
        return rs.getString(OPERATION);
    }

    public static String getUsername(ResultSet rs) throws SQLException {
        return rs.getString(USERNAME);
    }

    public static int getOperationCode(ResultSet rs) throws SQLException {
        return rs.getInt(OPERATION_CODE);
    }

    public static String getTableName(ResultSet rs) throws SQLException {
        return rs.getString(TABLE_NAME);
    }

    public static String getSegOwner(ResultSet rs) throws SQLException {
        return rs.getString(SEG_OWNER);
    }

    public static Timestamp getChangeTime(ResultSet rs) throws SQLException {
        // CHANGE_TIME is in the database server time zone but does not store the time zone.
        // Without passing any calendar the timestamp is considered to be in the JVM time zone.
        // This would lead to incorrect conversions when calling e.g. Timestamp#toInstant() if database
        // server time zone is not the same as JVM time zone. By passing UTC calendar CHANGE_TIME is considered
        // to be in UTC and no conversion happens on Timestamp#toInstant().
        return rs.getTimestamp(CHANGE_TIME, UTC_CALENDAR);
    }

    public static Scn getScn(ResultSet rs) throws SQLException {
        final String scn = rs.getString(SCN);
        if (scn == null) {
            return Scn.NULL;
        }
        return Scn.valueOf(scn);
    }

    public static String getTransactionId(ResultSet rs) throws SQLException {
        return HexConverter.convertToHexString(rs.getBytes(TX_ID));
    }

    /**
     * It constructs REDO_SQL. If REDO_SQL  is in a few lines, it truncates after first 40_000 characters
     * It also records LogMiner history info if isDml is true
     *
     * @param rs result set
     * @param isDml flag indicating if operation code is a DML
     * @param historyRecorder history recorder
     * @param scn scn
     * @param tableName table name
     * @param segOwner segment owner
     * @param operationCode operation code
     * @param changeTime time of change
     * @param txId transaction ID
     * @return the redo SQL
     * @throws SQLException if an exception occurred while interacting with the data source
     */
    public static String getSqlRedo(ResultSet rs, boolean isDml, HistoryRecorder historyRecorder, Scn scn, String tableName,
                                    String segOwner, int operationCode, Timestamp changeTime, String txId)
            throws SQLException {
        int lobLimitCounter = 9; // todo : decide on approach ( XStream chunk option) and Lob limit

        String redoSql = rs.getString(SQL_REDO);
        if (redoSql == null) {
            return null;
        }

        StringBuilder result = new StringBuilder(redoSql);
        int csf = rs.getInt(CSF);
        if (isDml) {
            historyRecorder.record(scn, tableName, segOwner, operationCode, changeTime, txId, csf, redoSql);
        }

        // 0 - indicates SQL_REDO is contained within the same row
        // 1 - indicates that either SQL_REDO is greater than 4000 bytes in size and is continued in
        // the next row returned by the ResultSet
        while (csf == 1) {
            rs.next();
            if (lobLimitCounter-- == 0) {
                LOGGER.warn("LOB value was truncated due to the connector limitation of {} MB", 40);
                break;
            }

            redoSql = rs.getString(SQL_REDO);
            result.append(redoSql);
            csf = rs.getInt(CSF);
            if (isDml) {
                historyRecorder.record(scn, tableName, segOwner, operationCode, changeTime, txId, csf, redoSql);
            }
        }

        return result.toString();
    }

    public static String getRowId(ResultSet rs) throws SQLException {
        return rs.getString(ROW_ID);
    }

    public static int getRollbackFlag(ResultSet rs) throws SQLException {
        return rs.getInt(ROLLBACK_FLAG);
    }

    public static TableId getTableId(String catalogName, ResultSet rs) throws SQLException {
        return new TableId(catalogName, rs.getString(SEG_OWNER), rs.getString(TABLE_NAME));
    }

    public static Object getRsId(ResultSet rs) throws SQLException {
        return rs.getObject(RS_ID);
    }
}
