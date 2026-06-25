/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

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
    private final TableId dataCollectionId;
    private final EventDispatcher<${connectorName}Partition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;

    ${connectorName}StreamingChangeEventSource(${connectorName}ConnectorConfig config,
                                               TableId dataCollectionId,
                                               EventDispatcher<${connectorName}Partition, TableId> dispatcher,
                                               ErrorHandler errorHandler,
                                               Clock clock) {
        this.config = config;
        this.dataCollectionId = dataCollectionId;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
    }

    @Override
    public void execute(ChangeEventSource.ChangeEventSourceContext context,
                        ${connectorName}Partition partition,
                        ${connectorName}OffsetContext offsetContext) throws InterruptedException {

        LOGGER.info("Starting ${connectorName} streaming from position {}", offsetContext.getPosition());

        while (context.isRunning()) {
            // TODO: poll or watch the data source for new change events.
            // For each event, determine the operation and read the raw row, then dispatch:
            //   Envelope.Operation operation = ...; // CREATE, UPDATE, or DELETE
            //   dispatcher.dispatchDataChangeEvent(partition, dataCollectionId,
            //       new ${connectorName}ChangeRecordEmitter(
            //           partition, offsetContext, operation, rawRowData, clock, config));
            // Then advance offsetContext.setPosition(...) and persist it.

            // Remove this placeholder sleep once real polling is implemented.
            Thread.sleep(1_000);
        }

        LOGGER.info("${connectorName} streaming stopped");
    }
}
