/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Collect;

class SqlServerEventMetadataProvider implements EventMetadataProvider {

    @Override
    public long getEventTimestamp(Object source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return -1;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return -1;
        }
        final Long timestamp = sourceInfo.getInt64(SourceInfo.LOG_TIMESTAMP_KEY);
        return timestamp == null ? -1 : timestamp;
    }

    @Override
    public Map<String, String> getEventSourcePosition(Object source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        return Collect.hashMapOf(
                SourceInfo.COMMIT_LSN_KEY, sourceInfo.getString(SourceInfo.COMMIT_LSN_KEY),
                SourceInfo.CHANGE_LSN_KEY, sourceInfo.getString(SourceInfo.CHANGE_LSN_KEY)
        );
    }

    @Override
    public String getTransactionId(Object source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        return sourceInfo.getString(SourceInfo.COMMIT_LSN_KEY);
    }
}
