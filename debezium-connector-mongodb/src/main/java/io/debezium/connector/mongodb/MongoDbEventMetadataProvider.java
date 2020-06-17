/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Collect;

/**
 * An {@link EventMetadataProvider} implementation for Mongodb to extract metrics data from events.
 *
 * @author Chris Cranford
 */
public class MongoDbEventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        if (sourceInfo.schema().field(SourceInfo.TIMESTAMP) != null) {
            final Integer timestamp = sourceInfo.getInt32(SourceInfo.TIMESTAMP);
            return timestamp == null ? null : Instant.ofEpochSecond(timestamp);
        }
        final Long timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_KEY);
        return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
    }

    @Override
    public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }

        Integer ord = sourceInfo.getInt32(SourceInfo.ORDER);
        return Collect.hashMapOf(SourceInfo.ORDER, Integer.toString(ord));
    }

    @Override
    public String getTransactionId(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        if (sourceInfo.schema().field(SourceInfo.SESSION_TXN_ID) != null) {
            final String sessionTxnId = sourceInfo.getString(SourceInfo.SESSION_TXN_ID);
            if (sessionTxnId != null) {
                return sessionTxnId;
            }
        }
        final Long operationId = sourceInfo.getInt64(SourceInfo.OPERATION_ID);
        if (operationId == null) {
            return null;
        }
        return Long.toString(operationId);
    }
}
