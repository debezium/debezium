/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.pipeline.spi.OffsetContext;

public class OracleOffsetContext implements OffsetContext {

    private static final String SERVER_PARTITION_KEY = "server";

    private final Schema sourceInfoSchema;
    private final Map<String, String> partition;

    private final SourceInfo sourceInfo;

    public OracleOffsetContext(String serverName) {
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, serverName);
        sourceInfo = new SourceInfo(serverName);
        sourceInfoSchema = sourceInfo.schema();
    }

    @Override
    public Map<String, ?> getPartition() {
        return partition;
    }

    @Override
    public Map<String, ?> getOffset() {
        return Collections.singletonMap(SourceInfo.POSITION_KEY, sourceInfo.getPosition());
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    public void setPosition(byte[] position) {
        sourceInfo.setPosition(position);
    }

    public byte[] getPosition() {
        return sourceInfo.getPosition();
    }

    public void setTransactionId(String transactionId) {
        sourceInfo.setTransactionId(transactionId);
    }

    public void setSourceTime(Instant instant) {
        sourceInfo.setSourceTime(instant);
    }
}
