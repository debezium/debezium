/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.ResultSet;
import java.sql.SQLException;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;

/**
 * Pre-computed JDBC {@link java.sql.ResultSet} column ordinals for the LogMiner query.
 *
 * <p>Five query columns are optional; when disabled they are omitted from the {@code SELECT},
 * shifting every subsequent column's ordinal. This class resolves those shifts <em>once</em> at
 * startup (via {@link #fromConfig}) rather than on every row.
 *
 * <p>Positions 1–9 are always present and exposed as {@code public static final} constants.
 * Positions 10 onwards vary by configuration and are stored as instance fields, with {@code null}
 * indicating the column is disabled.
 *
 * <p>The column ordering must exactly mirror {@link AbstractLogMinerQueryBuilder#buildColumnList()}.
 *
 * @author Debezium Authors
 */
public final class LogMinerColumnIndexes {

    /** ResultSet ordinal for {@code SCN}. */
    public static final int SCN = 1;
    /** ResultSet ordinal for {@code SQL_REDO}. */
    public static final int SQL_REDO = 2;
    /** ResultSet ordinal for {@code OPERATION_CODE}. */
    public static final int OPERATION_CODE = 3;
    /** ResultSet ordinal for {@code TIMESTAMP} (the change timestamp). */
    public static final int TIMESTAMP = 4;
    /** ResultSet ordinal for {@code XID} (transaction identifier bytes). */
    public static final int XID = 5;
    /** ResultSet ordinal for {@code CSF} (continuation flag). */
    public static final int CSF = 6;
    /** ResultSet ordinal for {@code TABLE_NAME}. */
    public static final int TABLE_NAME = 7;
    /** ResultSet ordinal for {@code SEG_OWNER} (used as the tablespace / schema name). */
    public static final int SEG_OWNER = 8;
    /** ResultSet ordinal for {@code OPERATION}. */
    public static final int OPERATION = 9;

    private final ResultSetValueResolver[] resolvers;

    private final String catalogName;

    /** Ordinal for {@code USERNAME}, or {@code null} when username tracking is disabled. */
    private final Integer usernameIndex;
    /** Ordinal for {@code RS_ID}, or {@code null} when RS_ID tracking is disabled. */
    private final Integer rsIdIndex;
    /** Ordinal for {@code CLIENT_ID}, or {@code null} when client-ID tracking is disabled. */
    private final Integer clientIdIndex;
    /** Ordinal for {@code START_TIMESTAMP}, or {@code null} when start-timestamp tracking is disabled. */
    private final Integer startTimestampIndex;
    /** Ordinal for {@code COMMIT_TIMESTAMP}, or {@code null} when commit-timestamp tracking is disabled. */
    private final Integer commitTimestampIndex;

    private final int rowIdIndex;
    private final int rollbackFlagIndex;
    private final int statusIndex;
    private final int infoIndex;
    private final int ssnIndex;
    private final int threadIndex;
    private final int objectIdIndex;
    private final int objectVersionIndex;
    private final int dataObjectIdIndex;
    private final int startScnIndex;
    private final int commitScnIndex;
    private final int sequenceIndex;

    /**
     * Computes all column ordinals from the given configuration flags.
     *
     * <p>The algorithm mirrors the column-list construction in
     * {@link AbstractLogMinerQueryBuilder#buildColumnList()} exactly: walk the columns in order,
     * assigning the next sequential 1-based position to each column that is included in the query.
     */
    private LogMinerColumnIndexes(String catalogName,
                                  boolean trackUsername,
                                  boolean trackRsId,
                                  boolean trackClientId,
                                  boolean trackStartTimestamp,
                                  boolean trackCommitTimestamp) {
        this.catalogName = catalogName;

        int pos = OPERATION;

        // Optional: USERNAME
        if (trackUsername) {
            usernameIndex = ++pos;
        }
        else {
            usernameIndex = null;
        }

        // Mandatory: ROW_ID, ROLLBACK
        rowIdIndex = ++pos;
        rollbackFlagIndex = ++pos;

        // Optional: RS_ID
        if (trackRsId) {
            rsIdIndex = ++pos;
        }
        else {
            rsIdIndex = null;
        }

        // Mandatory block: STATUS … DATA_OBJD#
        statusIndex = ++pos;
        infoIndex = ++pos;
        ssnIndex = ++pos;
        threadIndex = ++pos;
        objectIdIndex = ++pos;
        objectVersionIndex = ++pos;
        dataObjectIdIndex = ++pos;

        // Optional: CLIENT_ID
        if (trackClientId) {
            clientIdIndex = ++pos;
        }
        else {
            clientIdIndex = null;
        }

        // Mandatory: START_SCN, COMMIT_SCN
        startScnIndex = ++pos;
        commitScnIndex = ++pos;

        // Optional: START_TIMESTAMP
        if (trackStartTimestamp) {
            startTimestampIndex = ++pos;
        }
        else {
            startTimestampIndex = null;
        }

        // Optional: COMMIT_TIMESTAMP
        if (trackCommitTimestamp) {
            commitTimestampIndex = ++pos;
        }
        else {
            commitTimestampIndex = null;
        }

        // Mandatory: SEQUENCE#
        sequenceIndex = ++pos;

        this.resolvers = LogMinerEventRow.buildResolvers(this);
    }

    /**
     * Creates a {@link LogMinerColumnIndexes} instance for the given connector configuration.
     * Call once at startup and reuse the returned instance for the lifetime of the streaming session.
     *
     * @param config the connector configuration, must not be {@code null}
     * @return an immutable {@code LogMinerColumnIndexes}
     */
    public static LogMinerColumnIndexes fromConfig(OracleConnectorConfig config) {
        return new LogMinerColumnIndexes(
                config.getCatalogName(),
                config.isLogMiningBufferTrackUsername(),
                config.isLogMiningBufferTrackRsId(),
                config.isLogMiningBufferTrackClientId(),
                config.isLogMiningBufferTrackStartTimestamp(),
                config.isLogMiningBufferTrackCommitTimestamp());
    }

    /**
     * Applies each resolver in the pre-built array to {@code row}.
     *
     * @param row the target row being populated, never {@code null}
     * @param rs  the current JDBC result set, never {@code null}
     * @throws SQLException if the result set cannot be read
     */
    public void applyResolvers(LogMinerEventRow row, ResultSet rs) throws SQLException {
        for (ResultSetValueResolver resolver : resolvers) {
            resolver.resolve(row, rs);
        }
    }

    /** For testing only: returns the number of {@link ResultSetValueResolver}s in the pre-built array. */
    int getResolverCount() {
        return resolvers.length;
    }

    /** Returns the catalog (database) name, used when constructing {@link io.debezium.relational.TableId}s. */
    public String getCatalogName() {
        return catalogName;
    }

    /**
     * Returns the 1-based ordinal for {@code USERNAME}, or {@code null} if username tracking is disabled
     * and the column is absent from the query.
     */
    public Integer getUsernameIndex() {
        return usernameIndex;
    }

    /** Returns the 1-based ordinal for {@code ROW_ID}. */
    public int getRowIdIndex() {
        return rowIdIndex;
    }

    /** Returns the 1-based ordinal for {@code ROLLBACK}. */
    public int getRollbackFlagIndex() {
        return rollbackFlagIndex;
    }

    /**
     * Returns the 1-based ordinal for {@code RS_ID}, or {@code null} if RS-ID tracking is disabled
     * and the column is absent from the query.
     */
    public Integer getRsIdIndex() {
        return rsIdIndex;
    }

    /** Returns the 1-based ordinal for {@code STATUS}. */
    public int getStatusIndex() {
        return statusIndex;
    }

    /** Returns the 1-based ordinal for {@code INFO}. */
    public int getInfoIndex() {
        return infoIndex;
    }

    /** Returns the 1-based ordinal for {@code SSN}. */
    public int getSsnIndex() {
        return ssnIndex;
    }

    /** Returns the 1-based ordinal for {@code THREAD#}. */
    public int getThreadIndex() {
        return threadIndex;
    }

    /** Returns the 1-based ordinal for {@code DATA_OBJ#}. */
    public int getObjectIdIndex() {
        return objectIdIndex;
    }

    /** Returns the 1-based ordinal for {@code DATA_OBJV#}. */
    public int getObjectVersionIndex() {
        return objectVersionIndex;
    }

    /** Returns the 1-based ordinal for {@code DATA_OBJD#}. */
    public int getDataObjectIdIndex() {
        return dataObjectIdIndex;
    }

    /**
     * Returns the 1-based ordinal for {@code CLIENT_ID}, or {@code null} if client-ID tracking is
     * disabled and the column is absent from the query.
     */
    public Integer getClientIdIndex() {
        return clientIdIndex;
    }

    /** Returns the 1-based ordinal for {@code START_SCN}. */
    public int getStartScnIndex() {
        return startScnIndex;
    }

    /** Returns the 1-based ordinal for {@code COMMIT_SCN}. */
    public int getCommitScnIndex() {
        return commitScnIndex;
    }

    /**
     * Returns the 1-based ordinal for {@code START_TIMESTAMP}, or {@code null} if start-timestamp
     * tracking is disabled and the column is absent from the query.
     */
    public Integer getStartTimestampIndex() {
        return startTimestampIndex;
    }

    /**
     * Returns the 1-based ordinal for {@code COMMIT_TIMESTAMP}, or {@code null} if commit-timestamp
     * tracking is disabled and the column is absent from the query.
     */
    public Integer getCommitTimestampIndex() {
        return commitTimestampIndex;
    }

    /** Returns the 1-based ordinal for {@code SEQUENCE#}. */
    public int getSequenceIndex() {
        return sequenceIndex;
    }
}
