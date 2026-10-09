/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.util.Map;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;

/**
 * Restores a {@link ${connectorName}OffsetContext} from Kafka Connect's persisted offset storage.
 *
 * <p>Called once on connector start. When {@code offset} is null or empty the connector
 * has never run before and a full snapshot should be performed. Otherwise, streaming
 * resumes from the stored position.
 */
public class ${connectorName}OffsetLoader implements OffsetContext.Loader<${connectorName}OffsetContext> {

    private final ${connectorName}ConnectorConfig config;

    public ${connectorName}OffsetLoader(${connectorName}ConnectorConfig config) {
        this.config = config;
    }

    @Override
    public ${connectorName}OffsetContext load(Map<String, ?> offset) {
        final ${connectorName}SourceInfo sourceInfo = new ${connectorName}SourceInfo(config);

        if (offset == null || offset.isEmpty()) {
            return new ${connectorName}OffsetContext(sourceInfo);
        }

        final ${connectorName}OffsetContext ctx = new ${connectorName}OffsetContext(
                sourceInfo,
                loadSnapshot(offset).orElse(null),
                loadSnapshotCompleted(offset),
                TransactionContext.load(offset));
        ctx.setPosition(((Number) offset.get(${connectorName}OffsetContext.POSITION_KEY)).longValue());
        return ctx;
    }
}
