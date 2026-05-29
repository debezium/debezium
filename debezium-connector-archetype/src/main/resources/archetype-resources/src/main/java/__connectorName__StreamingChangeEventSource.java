/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;

/**
 * Streams ongoing changes from the ${connectorName} data source.
 *
 * <p>Implement the {@link #execute} method to tail/poll the source for new events and
 * dispatch them via the {@link EventDispatcher}. The loop must check
 * {@link ChangeEventSource.ChangeEventSourceContext#isRunning()} and exit cleanly when
 * it returns {@code false}.
 */
class ${connectorName}StreamingChangeEventSource
        implements StreamingChangeEventSource<${connectorName}Partition, ${connectorName}OffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(${connectorName}StreamingChangeEventSource.class);

    private final ${connectorName}ConnectorConfig config;
    private final ${connectorName}DataCollectionId dataCollectionId;
    private final EventDispatcher<${connectorName}Partition, ${connectorName}DataCollectionId> dispatcher;

    ${connectorName}StreamingChangeEventSource(${connectorName}ConnectorConfig config,
                                               ${connectorName}DataCollectionId dataCollectionId,
                                               EventDispatcher<${connectorName}Partition, ${connectorName}DataCollectionId> dispatcher) {
        this.config = config;
        this.dataCollectionId = dataCollectionId;
        this.dispatcher = dispatcher;
    }

    @Override
    public void execute(ChangeEventSource.ChangeEventSourceContext context,
                        ${connectorName}Partition partition,
                        ${connectorName}OffsetContext offsetContext) throws InterruptedException {

        LOGGER.info("Starting ${connectorName} streaming from position {}", offsetContext.getPosition());

        while (context.isRunning()) {
            // TODO: poll or watch the data source for new change events.
            // For each new event, call:
            //   dispatcher.dispatchDataChangeEvent(partition, dataCollectionId,
            //       new ${connectorName}ChangeRecordEmitter(partition, offsetContext, operation, key, value));
            // Then advance offsetContext.setPosition(...) and persist it.

            // Remove this placeholder sleep once real polling is implemented.
            Thread.sleep(1_000);
        }

        LOGGER.info("${connectorName} streaming stopped");
    }
}
