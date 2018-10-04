/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.time.Instant;

import io.debezium.annotation.NotThreadSafe;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfo;

@NotThreadSafe
public class SourceInfo extends AbstractSourceInfo {

    public static final String SERVER_NAME_KEY = "name";
    public static final String TXID_KEY = "txId";
    public static final String TIMESTAMP_KEY = "ts_ms";
    public static final String SCN_KEY = "scn";
    public static final String SNAPSHOT_KEY = "snapshot";

    public static final Schema SCHEMA = schemaBuilder()
            .name("io.debezium.connector.oracle.Source")
            .field(SERVER_NAME_KEY, Schema.STRING_SCHEMA)
            .field(TIMESTAMP_KEY, Schema.OPTIONAL_INT64_SCHEMA)
            .field(TXID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(SCN_KEY, Schema.OPTIONAL_INT64_SCHEMA)
            .field(SNAPSHOT_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .build();

    private final String serverName;
    private long scn;
    private String transactionId;
    private Instant sourceTime;
    private boolean snapshot;

    protected SourceInfo(String serverName) {
        super(Module.version());
        this.serverName = serverName;
    }

    @Override
    protected Schema schema() {
        return SCHEMA;
    }

    @Override
    protected String connector() {
        return Module.name();
    }

    @Override
    public Struct struct() {
        return super.struct()
                .put(SERVER_NAME_KEY, serverName)
                .put(TIMESTAMP_KEY, sourceTime.toEpochMilli())
                .put(TXID_KEY, transactionId)
                .put(SCN_KEY, scn)
                .put(SNAPSHOT_KEY, snapshot);
    }

    public String getServerName() {
        return serverName;
    }

    public long getScn() {
        return scn;
    }

    public void setScn(long scn) {
        this.scn = scn;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public Instant getSourceTime() {
        return sourceTime;
    }

    public void setSourceTime(Instant sourceTime) {
        this.sourceTime = sourceTime;
    }

    public void setSnapshot(boolean snapshot) {
        this.snapshot = snapshot;
    }

    public boolean isSnapshot() {
        return snapshot;
    }
}
