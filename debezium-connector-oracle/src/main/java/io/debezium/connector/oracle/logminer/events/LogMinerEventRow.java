/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;
import io.debezium.util.HexConverter;
import io.debezium.util.Strings;

/**
 * A simple wrapper around a {@link ResultSet} for a given row.
 *
 * @author Chris Cranford
 */
public class LogMinerEventRow {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerEventRow.class);

    private static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC));

    /* Allows for up to 100KB worth of SQL */
    private static final Integer MAX_SQL_CONTINUATIONS = 25;

    private static final int SCN = 1;
    private static final int SQL_REDO = 2;
    private static final int OPERATION_CODE = 3;
    private static final int CHANGE_TIME = 4;
    private static final int TX_ID = 5;
    private static final int CSF = 6;
    private static final int TABLE_NAME = 7;
    private static final int TABLESPACE_NAME = 8;
    private static final int OPERATION = 9;
    private static final int USERNAME = 10;
    private static final int ROW_ID = 11;
    private static final int ROLLBACK_FLAG = 12;
    private static final int RS_ID = 13;
    private static final int STATUS = 14;
    private static final int INFO = 15;
    private static final int SSN = 16;
    private static final int THREAD = 17;

    private Scn scn;
    private TableId tableId;
    private String tableName;
    private String tablespaceName;
    private EventType eventType;
    private Instant changeTime;
    private String transactionId;
    private String operation;
    private String userName;
    private String rowId;
    private boolean rollbackFlag;
    private String rsId;
    private String redoSql;
    private int status;
    private String info;
    private long ssn;
    private int thread;

    public Scn getScn() {
        return scn;
    }

    public TableId getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTablespaceName() {
        return tablespaceName;
    }

    public EventType getEventType() {
        return eventType;
    }

    public Instant getChangeTime() {
        return changeTime;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getOperation() {
        return operation;
    }

    public String getUserName() {
        return userName;
    }

    public String getRowId() {
        return rowId;
    }

    public boolean isRollbackFlag() {
        return rollbackFlag;
    }

    public String getRsId() {
        return rsId;
    }

    public String getRedoSql() {
        return redoSql;
    }

    public int getStatus() {
        return status;
    }

    public String getInfo() {
        return info;
    }

    public long getSsn() {
        return ssn;
    }

    public int getThread() {
        return thread;
    }

    /**
     * Returns a {@link LogMinerEventRow} instance based on the current row of the JDBC {@link ResultSet}.
     *
     * It's important to note that the instance returned by this method is never created as a new instance. The
     * method uses an internal single instance that is initialized based on the values from the current row
     * of the JDBC result-set to avoid creating lots of intermediate objects.
     *
     * @param resultSet the result set to be read, should never be {@code null}
     * @param catalogName the catalog name, should never be {@code null}
     * @param isTxIdRawValue whether the transaction id should be read as a raw value or not
     * @return a populated instance of a LogMinerEventRow object.
     * @throws SQLException if there was a problem reading the result set
     */
    public static LogMinerEventRow fromResultSet(ResultSet resultSet, String catalogName, boolean isTxIdRawValue) throws SQLException {
        LogMinerEventRow row = new LogMinerEventRow();
        row.initializeFromResultSet(resultSet, catalogName, isTxIdRawValue);
        return row;
    }

    /**
     * Initializes the instance from the JDBC {@link ResultSet}.
     *
     * @param resultSet the result set to be read, should never be {@code null}
     * @param catalogName the catalog name, should never be {@code null}
     * @param isTxIdRawValue whether the transaction id should be read as a raw value or not
     * @throws SQLException if there was a problem reading the result set
     */
    private void initializeFromResultSet(ResultSet resultSet, String catalogName, boolean isTxIdRawValue) throws SQLException {
        // Initialize the state from the result set
        this.scn = getScn(resultSet);
        this.tableName = resultSet.getString(TABLE_NAME);
        this.tablespaceName = resultSet.getString(TABLESPACE_NAME);
        this.eventType = EventType.from(resultSet.getInt(OPERATION_CODE));
        this.changeTime = getChangeTime(resultSet);
        this.transactionId = getTransactionId(resultSet, isTxIdRawValue);
        this.operation = resultSet.getString(OPERATION);
        this.userName = resultSet.getString(USERNAME);
        this.rowId = resultSet.getString(ROW_ID);
        this.rollbackFlag = resultSet.getInt(ROLLBACK_FLAG) == 1;
        this.rsId = trim(resultSet.getString(RS_ID));
        this.redoSql = getSqlRedo(resultSet);
        this.status = resultSet.getInt(STATUS);
        this.info = resultSet.getString(INFO);
        this.ssn = resultSet.getLong(SSN);
        this.thread = resultSet.getInt(THREAD);
        if (this.tableName != null) {
            this.tableId = new TableId(catalogName, tablespaceName, tableName);
        }
    }

    private String getTransactionId(ResultSet rs, boolean asRawValue) throws SQLException {
        if (asRawValue) {
            byte[] result = rs.getBytes(TX_ID);
            return result != null ? HexConverter.convertToHexString(result) : null;
        }
        return rs.getString(TX_ID);
    }

    private Instant getChangeTime(ResultSet rs) throws SQLException {
        final Timestamp result = rs.getTimestamp(CHANGE_TIME, UTC_CALENDAR);
        return result != null ? result.toInstant() : null;
    }

    private Scn getScn(ResultSet rs) throws SQLException {
        final String scn = rs.getString(SCN);
        return Strings.isNullOrEmpty(scn) ? Scn.NULL : Scn.valueOf(scn);
    }

    private String getSqlRedo(ResultSet rs) throws SQLException {
        String redoSql = rs.getString(SQL_REDO);
        if (redoSql == null) {
            return null;
        }

        StringBuilder result = new StringBuilder(redoSql);
        int csf = rs.getInt(CSF);
        int operationCode = rs.getInt(OPERATION_CODE);

        // 0 - indicates SQL_REDO is contained within the same row
        // 1 - indicates that either SQL_REDO is greater than 4000 bytes in size and is continued in
        // the next row returned by the ResultSet
        long sqlLimitCounter = 0;
        while (csf == 1) {

            rs.next();
            sqlLimitCounter++;

            // The old behavior would break the loop and this could leave the connector in an obscure place
            // during the result-set traversal. this new code instead simply logs a warning and continues
            // to append the data to the buffer, with a warning when MAX_SQL_CONTINUATIONS happens. This
            // should give some indication in the logs if an OOM occurs as to the result.
            if (sqlLimitCounter == MAX_SQL_CONTINUATIONS) {
                // We specifically only log warnings for LOB_WRITE and XML_WRITE operations because in theory
                // a standard SQL statement with text columns shouldn't be 100KB+ in length generally and if
                // so, the SQL statement will be trimmed down to its unique column name/values during the
                // parsing phase anyway. This issue is typically more problematic with LOB and XML.
                if (operationCode == EventType.LOB_WRITE.getValue()) {
                    LOGGER.warn("A large LOB write operation for table '{}' has been detected that exceeds {}kb.",
                            tableName, MAX_SQL_CONTINUATIONS * 4000);
                }
                else if (operationCode == EventType.XML_WRITE.getValue()) {
                    LOGGER.warn("A large XML write operation for table '{}' has been detected that exceeds {}kb.",
                            tableName, MAX_SQL_CONTINUATIONS * 4000);
                }
            }
            else if (sqlLimitCounter > Integer.MAX_VALUE) {
                // If we have gotten to this point we have reached a SQL statement that exceeds a length of
                // 8.589934588x10^12, which frankly likely isn't supported by the database engine, but we
                // have added this as a safeguard.
                throw new LogMinerEventRowTooLargeException(tableName, sqlLimitCounter * 4000, scn);
            }

            redoSql = rs.getString(SQL_REDO);
            result.append(redoSql);
            csf = rs.getInt(CSF);
            operationCode = rs.getInt(OPERATION_CODE);
        }

        return result.toString();
    }

    private static String trim(String value) {
        if (value != null) {
            return Strings.trim(value);
        }
        return null;
    }

    @Override
    public String toString() {
        return "LogMinerEventRow{" +
                "scn=" + scn +
                ", tableId='" + tableId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", tablespaceName='" + tablespaceName + '\'' +
                ", eventType=" + eventType +
                ", changeTime=" + changeTime +
                ", transactionId='" + transactionId + '\'' +
                ", operation='" + operation + '\'' +
                ", userName='" + userName + '\'' +
                ", rowId='" + rowId + '\'' +
                ", rollbackFlag=" + rollbackFlag +
                ", rsId=" + rsId +
                ", ssn=" + ssn +
                // Specifically log SQL only if TRACE is enabled; otherwise omit for others
                ", redoSql='" + (LOGGER.isTraceEnabled() ? redoSql : "<omitted>") + '\'' +
                '}';
    }
}
