/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfo;

public class SourceInfo extends AbstractSourceInfo {

    public static final String SERVER_NAME_KEY = "name";
    public static final String TXID_KEY = "txId";
    public static final String TIMESTAMP_KEY = "ts_sec";
    public static final String POSITION_KEY = "position";
    public static final String SNAPSHOT_KEY = "snapshot";

    public static final Schema SCHEMA = schemaBuilder()
            .name("io.debezium.connector.oracle.Source")
            .field(SERVER_NAME_KEY, Schema.STRING_SCHEMA)
            .field(TIMESTAMP_KEY, Schema.OPTIONAL_INT64_SCHEMA)
            .field(TXID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(POSITION_KEY, Schema.OPTIONAL_BYTES_SCHEMA)
            .field(SNAPSHOT_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .build();

    private final String serverName;
    private byte[] position;
    private String transactionId;
    private Instant sourceTime;

    protected SourceInfo(String serverName) {
        super(Module.version());
        this.serverName = serverName;
    }

    @Override
    protected Schema schema() {
        return SCHEMA;
    }

    @Override
    public Struct struct() {
        return super.struct()
                .put(SERVER_NAME_KEY, serverName)
                .put(TIMESTAMP_KEY, sourceTime.toEpochMilli())
                .put(TXID_KEY, transactionId)
                .put(POSITION_KEY, position)
                .put(SNAPSHOT_KEY, false);
    }

    public String getServerName() {
        return serverName;
    }

    public byte[] getPosition() {
        return position;
    }

    public void setPosition(byte[] position) {
        this.position = position;
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
}
