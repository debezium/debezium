/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.TableId;

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
    public static final int COMMIT = 7;
    public static final int MISSING_SCN = 34;
    public static final int ROLLBACK = 36;

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
    // todo: add these for recording
    // private static final int ROW_ID = 9;
    // private static final int SESSION_NUMBER = 10;
    // private static final int SERIAL_NUMBER = 11;
    // private static final int RS_ID = 12;
    // private static final int SSN = 12;

    public static String getOperation(TransactionalBufferMetrics metrics, ResultSet rs) {
        try {
            return rs.getString(OPERATION);
        }
        catch (SQLException e) {
            logError(metrics, e, "OPERATION");
            return null;
        }
    }

    public static String getUsername(TransactionalBufferMetrics metrics, ResultSet rs) {
        try {
            return rs.getString(USERNAME);
        }
        catch (SQLException e) {
            logError(metrics, e, "USERNAME");
            return null;
        }
    }

    public static int getOperationCode(TransactionalBufferMetrics metrics, ResultSet rs) {
        try {
            return rs.getInt(OPERATION_CODE);
        }
        catch (SQLException e) {
            logError(metrics, e, "OPERATION_CODE");
            return 0;
        }
    }

    public static String getTableName(TransactionalBufferMetrics metrics, ResultSet rs) {
        try {
            return rs.getString(TABLE_NAME);
        }
        catch (SQLException e) {
            logError(metrics, e, "TABLE_NAME");
            return "";
        }
    }

    public static String getSegOwner(TransactionalBufferMetrics metrics, ResultSet rs) {
        try {
            return rs.getString(SEG_OWNER);
        }
        catch (SQLException e) {
            logError(metrics, e, "SEG_OWNER");
            return "";
        }
    }

    public static Timestamp getChangeTime(TransactionalBufferMetrics metrics, ResultSet rs) {
        try {
            return rs.getTimestamp(CHANGE_TIME);
        }
        catch (SQLException e) {
            logError(metrics, e, "CHANGE_TIME");
            return new Timestamp(Instant.now().getEpochSecond());
        }
    }

    public static BigDecimal getScn(TransactionalBufferMetrics metrics, ResultSet rs) {
        try {
            return rs.getBigDecimal(SCN);
        }
        catch (SQLException e) {
            logError(metrics, e, "SCN");
            return new BigDecimal(-1);
        }
    }

    public static String getTransactionId(TransactionalBufferMetrics metrics, ResultSet rs) {
        try {
            return DatatypeConverter.printHexBinary(rs.getBytes(TX_ID));
        }
        catch (SQLException e) {
            logError(metrics, e, "TX_ID");
            return "";
        }
    }

    /**
     * It constructs REDO_SQL. If REDO_SQL  is in a few lines, it truncates after first 40_000 characters
     * It also records Log Miner history info if isDml is true
     *
     * @param metrics metrics
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
     */
    public static String getSqlRedo(TransactionalBufferMetrics metrics, ResultSet rs, boolean isDml,
                                    LogMinerHistoryRecorder historyRecorder, BigDecimal scn, String tableName,
                                    String segOwner, int operationCode, Timestamp changeTime, String txId) {
        int lobLimitCounter = 9; // todo : decide on approach ( XStream chunk option) and Lob limit
        StringBuilder result = new StringBuilder(4000);
        try {
            String redoSql = rs.getString(SQL_REDO);
            if (redoSql == null) {
                return null;
            }
            result = new StringBuilder(redoSql);

            int csf = rs.getInt(CSF);
            if (isDml) {
                historyRecorder.insertIntoTempTable(scn, tableName, segOwner, operationCode, changeTime, txId, csf, redoSql);
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
                result.append(rs.getString(SQL_REDO));
                csf = rs.getInt(CSF);
                if (isDml) {
                    historyRecorder.insertIntoTempTable(scn, tableName, segOwner, operationCode, changeTime, txId, csf, rs.getString(SQL_REDO));
                }
            }

        }
        catch (SQLException e) {
            logError(metrics, e, "SQL_REDO");
        }
        return result.toString();
    }

    private static void logError(TransactionalBufferMetrics metrics, SQLException e, String s) {
        LogMinerHelper.logError(metrics, "Cannot get {}. This entry from log miner will be lost due to the {}", s, e);
    }

    public static TableId getTableId(String catalogName, ResultSet rs) throws SQLException {
        return new TableId(catalogName, rs.getString(SEG_OWNER), rs.getString(TABLE_NAME));
    }

}
