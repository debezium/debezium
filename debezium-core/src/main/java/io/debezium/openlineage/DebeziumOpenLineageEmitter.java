/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.BaseSourceTask;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent.EventType;

import java.time.ZonedDateTime;

public class DebeziumOpenLineageEmitter {

    private static OpenLineageContext openLineageContext;
    private static OpenLineageEventEmitter emitter;
    private static Configuration config;

    public static void init(Configuration configuration) {
        config = configuration;
        emitter = new OpenLineageEventEmitter(configuration);
        openLineageContext = new OpenLineageContext(
                new OpenLineage(emitter.getProducer()),
                configuration.subset("openlineage.integration", false),
                new OpenLineageJobIdentifier(configuration.getString(CommonConnectorConfig.TOPIC_PREFIX),
                        configuration.getString(CommonConnectorConfig.TOPIC_PREFIX)));
    }

    public static void emit(BaseSourceTask.State state) {

        if (emitter.isEnabled()) {

            OpenLineage.Job job = new OpenLineageJobCreator(openLineageContext).create();

            OpenLineage.RunFacets runFacets = openLineageContext.getOpenLineage().newRunFacetsBuilder()
                    // TODO it will be good if the name could be debezium-connector, debezium-engine, debezium-server
                    .processing_engine(openLineageContext.getOpenLineage()
                            .newProcessingEngineRunFacet(
                                    ProcessingEngineMetadata.debezium().version(), ProcessingEngineMetadata.debezium().name(),
                                    ProcessingEngineMetadata.debezium().openlineageAdapterVersion()))
                    .nominalTime(
                            openLineageContext.getOpenLineage().newNominalTimeRunFacetBuilder()
                                    .nominalStartTime(ZonedDateTime.now())
                                    .nominalEndTime(ZonedDateTime.now())
                                    .build())
                    // TODO try to use also the .errorMessage()
                    .put(DebeziumConfigFacet.FACET_KEY_NAME, new DebeziumConfigFacet(emitter.getProducer(), config.asMap()))
                    .build();

            OpenLineage.RunEvent startEvent = openLineageContext.getOpenLineage().newRunEventBuilder()
                    .eventType(getEventType(state))
                    .eventTime(ZonedDateTime.now())
                    .run(openLineageContext.getOpenLineage()
                            .newRun(openLineageContext.getRunUuid(), runFacets))
                    .job(job)
                    .build();

            emitter.emit(startEvent);
        }
    }

    private static EventType getEventType(BaseSourceTask.State state) {
        return switch (state) {
            case INITIAL -> EventType.START;
            case RUNNING -> EventType.RUNNING;
            case RESTARTING -> EventType.FAIL;
            case STOPPED -> EventType.COMPLETE;
        };
    }
}
