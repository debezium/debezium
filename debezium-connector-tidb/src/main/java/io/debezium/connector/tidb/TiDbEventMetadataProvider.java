/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

/**
 * Provides metadata of TiDB change events for monitoring and transaction topics.
 *
 * @author Aviral Srivastava
 */
public class TiDbEventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.schema().field(Envelope.FieldName.SOURCE) != null
                ? value.getStruct(Envelope.FieldName.SOURCE)
                : null;
        if (sourceInfo == null) {
            return null;
        }
        final Long timestamp = sourceInfo.getInt64(AbstractSourceInfo.TIMESTAMP_KEY);
        return timestamp != null ? Instant.ofEpochMilli(timestamp) : null;
    }

    @Override
    public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.schema().field(Envelope.FieldName.SOURCE) != null
                ? value.getStruct(Envelope.FieldName.SOURCE)
                : null;
        if (sourceInfo == null) {
            return null;
        }
        return Collect.hashMapOf(SourceInfo.COMMIT_TS_KEY, Long.toString(sourceInfo.getInt64(SourceInfo.COMMIT_TS_KEY)));
    }

    @Override
    public String getTransactionId(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        // TiCDC's Debezium output does not carry transaction boundaries; the TSO commit
        // timestamp identifies the transaction a change belongs to
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.schema().field(Envelope.FieldName.SOURCE) != null
                ? value.getStruct(Envelope.FieldName.SOURCE)
                : null;
        if (sourceInfo == null) {
            return null;
        }
        final Long commitTs = sourceInfo.getInt64(SourceInfo.COMMIT_TS_KEY);
        return commitTs != null ? Long.toString(commitTs) : null;
    }
}
