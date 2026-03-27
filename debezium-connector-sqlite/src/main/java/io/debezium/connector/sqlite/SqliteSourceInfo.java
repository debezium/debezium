/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.time.Instant;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.relational.TableId;

/**
 * Source information specific to the SQLite connector, included in each change event's
 * {@code source} block.
 *
 * @author Zihan Dai
 */
public class SqliteSourceInfo extends AbstractSourceInfo {

    public static final String WAL_POSITION_KEY = "wal_position";
    public static final String WAL_SALT1_KEY = "wal_salt1";
    public static final String WAL_SALT2_KEY = "wal_salt2";
    public static final String WAL_EPOCH_KEY = "wal_epoch";
    public static final String DATABASE_KEY = "db";

    private String database;
    private String table;
    private long walPosition;
    private int walSalt1;
    private int walSalt2;
    private long walEpoch;
    private Instant timestamp;

    protected SqliteSourceInfo(SqliteConnectorConfig connectorConfig) {
        super(connectorConfig);
        this.database = connectorConfig.getDatabasePath();
    }

    public void setTableId(TableId tableId) {
        this.table = tableId != null ? tableId.table() : null;
    }

    public void setWalPosition(long walPosition) {
        this.walPosition = walPosition;
    }

    public void setWalSalts(int salt1, int salt2) {
        this.walSalt1 = salt1;
        this.walSalt2 = salt2;
    }

    public void setWalEpoch(long epoch) {
        this.walEpoch = epoch;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public long getWalPosition() {
        return walPosition;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    @Override
    protected String database() {
        return database;
    }

    @Override
    protected Instant timestamp() {
        return timestamp;
    }
}
