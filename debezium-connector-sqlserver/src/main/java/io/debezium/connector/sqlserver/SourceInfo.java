/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.AbstractSourceInfo;

/**
 * Coordinates from the database log to establish the relation between the change streamed and the source log position.
 * Maps to {@code source} field in {@code Envelope}.
 *
 * @author Jiri Pechanec
 *
 */
@NotThreadSafe
public class SourceInfo extends AbstractSourceInfo {

    public static final String SERVER_NAME_KEY = "name";
    public static final String LOG_TIMESTAMP_KEY = "ts_ms";
    public static final String CHANGE_LSN_KEY = "change_lsn";
    public static final String COMMIT_LSN_KEY = "commit_lsn";
    public static final String SNAPSHOT_KEY = "snapshot";

    public static final Schema SCHEMA = schemaBuilder()
            .name("io.debezium.connector.sqlserver.Source")
            .field(SERVER_NAME_KEY, Schema.STRING_SCHEMA)
            .field(LOG_TIMESTAMP_KEY, Schema.OPTIONAL_INT64_SCHEMA)
            .field(CHANGE_LSN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(COMMIT_LSN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(SNAPSHOT_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .build();

    private final String serverName;

    private Lsn changeLsn;
    private Lsn commitLsn;
    private boolean snapshot;
    private Instant sourceTime;

    protected SourceInfo(String serverName) {
        super(Module.version());
        this.serverName = serverName;
    }

    /**
     * @param lsn - LSN of the change in the database log
     */
    public void setChangeLsn(Lsn lsn) {
        changeLsn = lsn;
    }

    public Lsn getChangeLsn() {
        return changeLsn;
    }

    public Lsn getCommitLsn() {
        return commitLsn;
    }

    /**
     * @param commitLsn - LSN of the {@code COMMIT} of the transaction whose part the change is
     */
    public void setCommitLsn(Lsn commitLsn) {
        this.commitLsn = commitLsn;
    }

    /**
     * @param instant a time at which the transaction commit was executed
     */
    public void setSourceTime(Instant instant) {
        sourceTime = instant;
    }

    public boolean isSnapshot() {
        return snapshot;
    }

    /**
     * @param snapshot - true if the source of even is snapshot phase, not the database log
     */
    public void setSnapshot(boolean snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    protected Schema schema() {
        return SCHEMA;
    }

    @Override
    protected String connector() {
        return Module.name();
    }

    /**
     * @return the coordinates encoded as a {@code Struct}
     */
    @Override
    public Struct struct() {
        final Struct ret = super.struct()
                .put(SERVER_NAME_KEY, serverName)
                .put(LOG_TIMESTAMP_KEY, sourceTime == null ? null : sourceTime.toEpochMilli())
                .put(SNAPSHOT_KEY, snapshot);

        if (changeLsn.isAvailable()) {
            ret.put(CHANGE_LSN_KEY, changeLsn.toString());
        }
        if (commitLsn != null) {
            ret.put(COMMIT_LSN_KEY, commitLsn.toString());
        }
        return ret;
    }
}
