/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.util.Map;

import io.debezium.pipeline.spi.OffsetContext;

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
        ${connectorName}SourceInfo sourceInfo = new ${connectorName}SourceInfo(config);
        ${connectorName}OffsetContext ctx = new ${connectorName}OffsetContext(sourceInfo);

        if (offset == null || offset.isEmpty()) {
            ctx.setPosition(0);
        }
        else {
            long position = ((Number) offset.get(${connectorName}OffsetContext.POSITION_KEY)).longValue();
            ctx.setPosition(position);
        }
        return ctx;
    }
}
