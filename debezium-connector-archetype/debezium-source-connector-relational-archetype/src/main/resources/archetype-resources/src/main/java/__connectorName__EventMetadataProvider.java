/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Provides event metadata (timestamp, source position, transaction ID) used by JMX
 * metrics and the transaction monitor.
 *
 * <p>Implement {@link #getEventTimestamp} and {@link #getEventSourcePosition} with
 * values meaningful to your data source.
 */
public class ${connectorName}EventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset,
                                     Object key, Struct value) {
        return Instant.now();
    }

    @Override
    public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset,
                                                      Object key, Struct value) {
        if (offset instanceof ${connectorName}OffsetContext ctx) {
            return Map.of("position", String.valueOf(ctx.getPosition()));
        }
        return Map.of();
    }

    @Override
    public String getTransactionId(DataCollectionId source, OffsetContext offset,
                                   Object key, Struct value) {
        return null;
    }
}
