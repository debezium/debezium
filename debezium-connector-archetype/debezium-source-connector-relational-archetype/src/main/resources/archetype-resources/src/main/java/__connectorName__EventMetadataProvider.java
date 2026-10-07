/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Provides event metadata (timestamp, source position, transaction ID) used by JMX
 * metrics and the transaction monitor.
 *
 * <p>The timestamp and position are read from the {@code source} block of each event, so they
 * are only as accurate as the values {@link ${connectorName}SourceInfo} supplies. Implement
 * {@link #getTransactionId} if your source has transaction ids.
 */
public class ${connectorName}EventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset,
                                     Object key, Struct value) {
        if (value == null) {
            return null;
        }
        // The timestamp comes from the event's source block, so the lag metrics reflect the source
        // timestamp that ${connectorName}SourceInfo supplies.
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        final Long timestamp = sourceInfo.getInt64(AbstractSourceInfo.TIMESTAMP_KEY);
        return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
    }

    @Override
    public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset,
                                                      Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        final Long position = sourceInfo.getInt64(${connectorName}OffsetContext.POSITION_KEY);
        return position == null ? null : Map.of(${connectorName}OffsetContext.POSITION_KEY, Long.toString(position));
    }

    @Override
    public String getTransactionId(DataCollectionId source, OffsetContext offset,
                                   Object key, Struct value) {
        return null;
    }
}
