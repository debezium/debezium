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

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.Table;
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

    private static final String INPUT_DATASET_NAMESPACE_FORMAT = "%s://%s:%s";
    private static final String JAVA = "Java";

    private final OpenLineageContext openLineageContext;
    private final String connectorName;
    private final OpenLineageEventEmitter emitter;
    private final Configuration config;

    public OpenLineageEmitter(String connectorName, Configuration config, OpenLineageContext openLineageContext, OpenLineageEventEmitter emitter) {
        this.openLineageContext = openLineageContext;
        this.connectorName = connectorName;
        this.emitter = emitter;
        this.config = config;
    }

    @Override
    public void emit(BaseSourceTask.State state) {

        emit(state, null, null);
    }

    @Override
    public void emit(BaseSourceTask.State state, Throwable t) {

        emit(state, null, t);
    }

    @Override
    public void emit(BaseSourceTask.State state, Table event) {

        emit(state, event, null);
    }

    @Override
    public void emit(BaseSourceTask.State state, Table event, Throwable t) {

        OpenLineage.Job job = new OpenLineageJobCreator(openLineageContext).create();

        List<OpenLineage.InputDataset> inputs = getInputDatasets(event);

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

        if (t != null) {
            StringWriter sw = new StringWriter();
            t.printStackTrace(new PrintWriter(sw));
            runFacetsBuilder.errorMessage(openLineageContext.getOpenLineage()
                    .newErrorMessageRunFacet(t.getMessage(), JAVA, sw.toString()));
        }

        OpenLineage.RunEvent startEvent = openLineageContext.getOpenLineage().newRunEventBuilder()
                .eventType(getEventType(state))
                .eventTime(ZonedDateTime.now())
                .run(openLineageContext.getOpenLineage().newRun(openLineageContext.getRunUuid(), runFacetsBuilder.build()))
                .inputs(inputs)
                .job(job)
                .build();

        emitter.emit(startEvent);

    }

    private List<OpenLineage.InputDataset> getInputDatasets(Table table) {

        if (table == null) {
            return List.of();
        }
        List<OpenLineage.SchemaDatasetFacetFields> datasetFields = table.columns().stream()
                .map(c -> openLineageContext.getOpenLineage()
                        .newSchemaDatasetFacetFieldsBuilder()
                        .name(c.name())
                        .type(c.typeName())
                        .description(c.comment())
                        .build())
                .toList();

        String datasetNamespace = String.format(INPUT_DATASET_NAMESPACE_FORMAT,
                extractNamespacePrefix(),
                config.getString(CommonConnectorConfig.DATABASE_CONFIG_PREFIX + JdbcConfiguration.HOSTNAME),
                config.getString(CommonConnectorConfig.DATABASE_CONFIG_PREFIX + JdbcConfiguration.PORT));

        return List.of(
                openLineageContext.getOpenLineage().newInputDatasetBuilder()
                        // See https://openlineage.io/docs/spec/naming
                        .namespace(datasetNamespace)
                        .name(table.id().identifier())
                        .facets(
                                // Maybe we can add just the https://openlineage.io/docs/spec/facets/dataset-facets/ownership
                                // configurable via connector configuration
                                openLineageContext.getOpenLineage().newDatasetFacetsBuilder()
                                        .schema(openLineageContext.getOpenLineage().newSchemaDatasetFacetBuilder()
                                                .fields(datasetFields)
                                                .build())

                                        .datasetType(openLineageContext.getOpenLineage().newDatasetTypeDatasetFacet("TABLE", ""))
                                        .build())
                        .build());
    }

    private String extractNamespacePrefix() {
        return switch (connectorName) {
            case "postgresql" -> "postgres";
            default -> connectorName;
        };
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
