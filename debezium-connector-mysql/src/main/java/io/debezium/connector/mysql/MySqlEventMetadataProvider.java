/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

class MySqlEventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        final Long timestamp = sourceInfo.getInt64(AbstractSourceInfo.TIMESTAMP_KEY);
        return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
    }

    @Override
    public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset, Object key,
                                                      Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        return Collect.hashMapOf(SourceInfo.BINLOG_FILENAME_OFFSET_KEY,
                sourceInfo.getString(SourceInfo.BINLOG_FILENAME_OFFSET_KEY), SourceInfo.BINLOG_POSITION_OFFSET_KEY,
                Long.toString(sourceInfo.getInt64(SourceInfo.BINLOG_POSITION_OFFSET_KEY)),
                SourceInfo.BINLOG_ROW_IN_EVENT_OFFSET_KEY,
                Integer.toString(sourceInfo.getInt32(SourceInfo.BINLOG_ROW_IN_EVENT_OFFSET_KEY)));
    }

    @Override
    public String getTransactionId(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        return ((MySqlOffsetContext) offset).getTransactionId();
    }
}
