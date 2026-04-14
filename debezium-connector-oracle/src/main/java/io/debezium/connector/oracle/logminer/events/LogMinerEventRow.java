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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogMinerColumnIndexes;
import io.debezium.connector.oracle.logminer.ResultSetValueResolver;
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

    // Column ordinals 1–21 are fixed and exposed as constants on LogMinerColumnIndexes.
    // Column ordinals 22+ are pre-computed at startup by LogMinerColumnIndexes.fromConfig()
    // because optional columns can be omitted from the SELECT, shifting all subsequent positions.

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
    private long objectId;
    private long objectVersion;
    private long dataObjectId;
    private String clientId;
    private Scn startScn;
    private Scn commitScn;
    private Instant startTime;
    private Instant commitTime;
    private Long transactionSequence;

    public Scn getScn() {
        return scn;
    }

    public TableId getTableId() {
        return tableId;
    }

    public void setTableId(TableId tableId) {
        this.tableId = tableId;
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

    public Long getTransactionSequence() {
        return transactionSequence;
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

    public long getObjectId() {
        return objectId;
    }

    public long getObjectVersion() {
        return objectVersion;
    }

    public long getDataObjectId() {
        return dataObjectId;
    }

    public String getClientId() {
        return clientId;
    }

    public Scn getStartScn() {
        return startScn;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Scn getCommitScn() {
        return commitScn;
    }

    public Instant getCommitTime() {
        return commitTime;
    }

    public boolean hasErrorStatus() {
        return status == 2;
    }

    /**
     * Builds the array of {@link ResultSetValueResolver} lambdas used by
     * {@link LogMinerColumnIndexes#applyResolvers} on every result-set row.
     * <p>
     * Each lambda closes over a pre-computed JDBC ordinal; optional columns are excluded when
     * their index is {@code null}.
     *
     * @param indexes the pre-computed column ordinals, must not be {@code null}
     * @return an ordered array of resolvers
     */
    public static ResultSetValueResolver[] buildOptionalResolvers(LogMinerColumnIndexes indexes) {
        final List<ResultSetValueResolver> resolvers = new ArrayList<>();

        if (indexes.getUsernameIndex() != null) {
            final int pos = indexes.getUsernameIndex();
            resolvers.add((row, rs) -> row.userName = rs.getString(pos));
        }
        if (indexes.getRsIdIndex() != null) {
            final int pos = indexes.getRsIdIndex();
            resolvers.add((row, rs) -> row.rsId = trim(rs.getString(pos)));
        }
        if (indexes.getClientIdIndex() != null) {
            final int pos = indexes.getClientIdIndex();
            resolvers.add((row, rs) -> row.clientId = rs.getString(pos));
        }
        if (indexes.getStartTimestampIndex() != null) {
            final int pos = indexes.getStartTimestampIndex();
            resolvers.add((row, rs) -> row.startTime = getTime(rs, pos));
        }
        if (indexes.getCommitTimestampIndex() != null) {
            final int pos = indexes.getCommitTimestampIndex();
            resolvers.add((row, rs) -> row.commitTime = getTime(rs, pos));
        }

        return resolvers.toArray(new ResultSetValueResolver[0]);
    }

    /**
     * Returns a {@link LogMinerEventRow} instance based on the current row of the JDBC {@link ResultSet}.
     *
     * @param resultSet the result set to be read, should never be {@code null}
     * @param schema the relational schema, can be {@code null}
     * @param columnIndexes the pre-computed column ordinals for this query configuration,
     *                      should not be {@code null}. Obtain via
     *                      {@link LogMinerColumnIndexes#fromConfig(io.debezium.connector.oracle.OracleConnectorConfig)}
     *                      once at startup.
     * @return a populated instance of a LogMinerEventRow object.
     * @throws SQLException if there was a problem reading the result set
     */
    public static LogMinerEventRow fromResultSet(ResultSet resultSet, OracleDatabaseSchema schema,
                                                 LogMinerColumnIndexes columnIndexes)
            throws SQLException {
        LogMinerEventRow row = new LogMinerEventRow();
        row.initializeFromResultSet(resultSet, schema, columnIndexes);
        return row;
    }

    /**
     * Initializes the instance from the JDBC {@link ResultSet} using pre-computed column ordinals.
     *
     * @param resultSet the result set to be read, should never be {@code null}
     * @param schema the relational schema, can be {@code null}
     * @param indexes the pre-computed column ordinals, should not be {@code null}
     * @throws SQLException if there was a problem reading the result set
     */
    private void initializeFromResultSet(ResultSet resultSet, OracleDatabaseSchema schema,
                                         LogMinerColumnIndexes indexes)
            throws SQLException {
        // Fixed positions 1–21: always present, never shift
        this.scn = getScn(resultSet, LogMinerColumnIndexes.SCN);
        this.tableName = resultSet.getString(LogMinerColumnIndexes.TABLE_NAME);
        this.tablespaceName = resultSet.getString(LogMinerColumnIndexes.SEG_OWNER);
        this.eventType = EventType.from(resultSet.getInt(LogMinerColumnIndexes.OPERATION_CODE));
        this.changeTime = getTime(resultSet, LogMinerColumnIndexes.TIMESTAMP);
        this.transactionId = getTransactionId(resultSet);
        this.operation = resultSet.getString(LogMinerColumnIndexes.OPERATION);
        this.rowId = resultSet.getString(LogMinerColumnIndexes.ROW_ID);
        this.rollbackFlag = resultSet.getInt(LogMinerColumnIndexes.ROLLBACK) == 1;
        this.status = resultSet.getInt(LogMinerColumnIndexes.STATUS);
        this.info = resultSet.getString(LogMinerColumnIndexes.INFO);
        this.ssn = resultSet.getLong(LogMinerColumnIndexes.SSN);
        this.thread = resultSet.getInt(LogMinerColumnIndexes.THREAD);
        this.objectId = resultSet.getLong(LogMinerColumnIndexes.DATA_OBJ);
        this.objectVersion = resultSet.getLong(LogMinerColumnIndexes.DATA_OBJV);
        this.dataObjectId = resultSet.getLong(LogMinerColumnIndexes.DATA_OBJD);
        this.startScn = getScn(resultSet, LogMinerColumnIndexes.START_SCN);
        this.commitScn = getScn(resultSet, LogMinerColumnIndexes.COMMIT_SCN);
        this.transactionSequence = resultSet.getLong(LogMinerColumnIndexes.SEQUENCE);

        // Variable positions and SQL redo: iterate over pre-built resolvers.
        indexes.applyResolvers(this, resultSet);

        // Explicitly read sqlRedo at the end of all other columns
        // getSqlRedo reads from fixed positions 2, 3, 6 and may call rs.next() for continuation rows.
        this.redoSql = getSqlRedo(resultSet);

        if (this.tableName != null) {
            if (schema != null) {
                this.tableId = schema.resolveTableId(indexes.getCatalogName(), tablespaceName, tableName);
            }
            else {
                this.tableId = new TableId(indexes.getCatalogName(), tablespaceName, tableName);
            }
        }
    }

    private static String getTransactionId(ResultSet rs) throws SQLException {
        byte[] result = rs.getBytes(LogMinerColumnIndexes.XID);
        return result != null ? HexConverter.convertToHexString(result) : null;
    }

    private static Instant getTime(ResultSet rs, int columnIndex) throws SQLException {
        final Timestamp result = rs.getTimestamp(columnIndex, UTC_CALENDAR);
        return result != null ? result.toInstant() : null;
    }

    private static Scn getScn(ResultSet rs, int columnIndex) throws SQLException {
        final String scn = rs.getString(columnIndex);
        return Strings.isNullOrEmpty(scn) ? Scn.NULL : Scn.valueOf(scn);
    }

    private String getSqlRedo(ResultSet rs) throws SQLException {
        int csf = rs.getInt(LogMinerColumnIndexes.CSF);
        // 0 - indicates SQL_REDO is contained within the same row
        if (csf == 0) {
            return rs.getString(LogMinerColumnIndexes.SQL_REDO);
        }
        int operationCode = rs.getInt(LogMinerColumnIndexes.OPERATION_CODE);
        StringBuilder result = new StringBuilder(rs.getString(LogMinerColumnIndexes.SQL_REDO));

        long sqlLimitCounter = 0;
        // 1 - indicates that either SQL_REDO is greater than 4000 bytes in size and is continued in
        // the next row returned by the ResultSet
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

            csf = rs.getInt(LogMinerColumnIndexes.CSF);
            operationCode = rs.getInt(LogMinerColumnIndexes.OPERATION_CODE);
            result.append(rs.getString(LogMinerColumnIndexes.SQL_REDO));
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
                ", objectId=" + objectId +
                ", objectVersion=" + objectVersion +
                ", dataObjectId=" + dataObjectId +
                ", startScn=" + startScn +
                ", commitScn=" + commitScn +
                ", operation='" + operation + '\'' +
                ", tableId='" + tableId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", tablespaceName='" + tablespaceName + '\'' +
                ", eventType=" + eventType +
                ", changeTime=" + changeTime +
                ", startTime=" + startTime +
                ", commitTime=" + commitTime +
                ", transactionId='" + transactionId + '\'' +
                ", operation='" + operation + '\'' +
                ", transactionSequence=" + transactionSequence +
                ", objectId=" + objectId +
                ", objectVersion=" + objectVersion +
                ", dataObjectId=" + dataObjectId +
                ", eventType=" + eventType +
                ", userName='" + userName + '\'' +
                ", rowId='" + rowId + '\'' +
                ", rollbackFlag=" + rollbackFlag +
                ", rsId=" + rsId +
                ", ssn=" + ssn +
                ", thread=" + thread +
                ", clientId=" + clientId +
                // Specifically log SQL only if TRACE is enabled; otherwise omit for others
                ", redoSql='" + (LOGGER.isTraceEnabled() ? redoSql : "<omitted>") + '\'' +
                '}';
    }
}
