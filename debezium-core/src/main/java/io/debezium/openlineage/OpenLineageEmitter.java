/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.ZonedDateTime;
import java.util.List;

import io.debezium.config.Configuration;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.openlineage.dataset.DatasetNamespaceResolver;
import io.debezium.openlineage.facets.DebeziumConfigFacet;
import io.openlineage.client.OpenLineage;

/**
 * Implementation of the LineageEmitter interface that sends data lineage events through OpenLineage API.
 * <p>
 * This class generates and emits OpenLineage-compatible events based on Debezium connector state,
 * table events, and exception information. The emitter constructs proper OpenLineage RunEvent objects
 * with appropriate metadata, facets, and dataset information derived from Debezium's internal state.
 * <p>
 * The emitter translates Debezium connector states to corresponding OpenLineage event types:
 * <ul>
 *   <li>INITIAL → START</li>
 *   <li>RUNNING → RUNNING</li>
 *   <li>RESTARTING → FAIL</li>
 *   <li>STOPPED → COMPLETE</li>
 * </ul>
 * <p>
 * For table events, the emitter extracts detailed schema information, including column names,
 * types, and descriptions, which are included as dataset facets in the OpenLineage events.
 * Database connection information is used to construct appropriate dataset namespace identifiers.
 *
 * @see LineageEmitter
 * @see OpenLineageContext
 * @see OpenLineageEventEmitter
 * @see BaseSourceTask.State
 */
public class OpenLineageEmitter implements LineageEmitter {

    private static final String JAVA = "Java";
    public static final String DATASET_TYPE = "TABLE";

    private final OpenLineageContext openLineageContext;
    private final String connectorName;
    private final OpenLineageEventEmitter emitter;
    private final Configuration config;
    private final DatasetNamespaceResolver datasetNamespaceResolver;

    public OpenLineageEmitter(String connectorName, Configuration config, OpenLineageContext openLineageContext, OpenLineageEventEmitter emitter,
                              DatasetNamespaceResolver datasetNamespaceResolver) {
        this.openLineageContext = openLineageContext;
        this.connectorName = connectorName;
        this.emitter = emitter;
        this.config = config;
        this.datasetNamespaceResolver = datasetNamespaceResolver;
    }

    @Override
    public void emit(BaseSourceTask.State state) {

        emit(state, List.of(), null);
    }

    @Override
    public void emit(BaseSourceTask.State state, Throwable t) {

        emit(state, List.of(), t);
    }

    @Override
    public void emit(BaseSourceTask.State state, List<DataCollectionMetadata> inputDatasetMetadata) {

        emit(state, inputDatasetMetadata, null);
    }

    @Override
    public void emit(BaseSourceTask.State state, List<DataCollectionMetadata> inputDatasetMetadata, Throwable t) {

        OpenLineage.Job job = new OpenLineageJobCreator(openLineageContext).create();

        List<OpenLineage.InputDataset> inputs = getInputDatasets(inputDatasetMetadata);

        OpenLineage.RunFacetsBuilder runFacetsBuilder = openLineageContext.getOpenLineage().newRunFacetsBuilder()
                // TODO it will be good if the name could be debezium-connector, debezium-engine, debezium-server
                .processing_engine(openLineageContext.getOpenLineage()
                        .newProcessingEngineRunFacet(
                                ProcessingEngineMetadata.debezium().version(),
                                ProcessingEngineMetadata.debezium().name(),
                                ProcessingEngineMetadata.debezium().openlineageAdapterVersion()))
                .nominalTime(
                        openLineageContext.getOpenLineage().newNominalTimeRunFacetBuilder()
                                .nominalStartTime(ZonedDateTime.now())
                                .nominalEndTime(ZonedDateTime.now())
                                .build())
                .put(DebeziumConfigFacet.FACET_KEY_NAME, new DebeziumConfigFacet(emitter.getProducer(), config.asMap()));

        addStackTrace(t, runFacetsBuilder);

        OpenLineage.RunEvent startEvent = openLineageContext.getOpenLineage().newRunEventBuilder()
                .eventType(getEventType(state))
                .eventTime(ZonedDateTime.now())
                .run(openLineageContext.getOpenLineage().newRun(openLineageContext.getRunUuid(), runFacetsBuilder.build()))
                .inputs(inputs)
                .job(job)
                .build();

        emitter.emit(startEvent);

    }

    private void addStackTrace(Throwable t, OpenLineage.RunFacetsBuilder runFacetsBuilder) {
        if (t != null) {
            StringWriter sw = new StringWriter();
            t.printStackTrace(new PrintWriter(sw));
            runFacetsBuilder.errorMessage(openLineageContext.getOpenLineage()
                    .newErrorMessageRunFacet(t.getMessage(), JAVA, sw.toString()));
        }
    }

    private List<OpenLineage.InputDataset> getInputDatasets(List<DataCollectionMetadata> inputDatasetMetadata) {

        return inputDatasetMetadata.stream()
                .map(this::mapToInputDataset)
                .toList();

    }

    private OpenLineage.InputDataset mapToInputDataset(DataCollectionMetadata dataCollectionMetadata) {

        List<OpenLineage.SchemaDatasetFacetFields> datasetFields = dataCollectionMetadata.fields().stream()
                .map(datasetMetadata -> openLineageContext.getOpenLineage()
                        .newSchemaDatasetFacetFieldsBuilder()
                        .name(datasetMetadata.name())
                        .type(datasetMetadata.typeName())
                        .description(datasetMetadata.description())
                        .build())
                .toList();

        return openLineageContext.getOpenLineage().newInputDatasetBuilder()
                .namespace(datasetNamespaceResolver.resolve(config, connectorName))
                .name(dataCollectionMetadata.id().identifier())
                .facets(
                        openLineageContext.getOpenLineage().newDatasetFacetsBuilder()
                                .schema(openLineageContext.getOpenLineage().newSchemaDatasetFacetBuilder()
                                        .fields(datasetFields)
                                        .build())
                                .datasetType(openLineageContext.getOpenLineage().newDatasetTypeDatasetFacet(DATASET_TYPE, ""))
                                .build())
                .build();
    }

    private static OpenLineage.RunEvent.EventType getEventType(BaseSourceTask.State state) {
        return switch (state) {
            case INITIAL -> OpenLineage.RunEvent.EventType.START;
            case RUNNING -> OpenLineage.RunEvent.EventType.RUNNING;
            case RESTARTING -> OpenLineage.RunEvent.EventType.FAIL;
            case STOPPED -> OpenLineage.RunEvent.EventType.COMPLETE;
        };
    }
}
