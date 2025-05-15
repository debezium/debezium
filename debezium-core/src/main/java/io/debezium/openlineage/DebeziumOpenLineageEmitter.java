/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.time.ZonedDateTime;
import java.util.List;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.Table;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent.EventType;

public class DebeziumOpenLineageEmitter {

    public static final String INPUT_DATASET_NAMESPACE_FORMAT = "%s://%s:%s";
    private static OpenLineageContext openLineageContext;
    private static String connectorName;
    private static OpenLineageEventEmitter emitter;
    private static Configuration config;

    public static void init(Configuration configuration, String connName) {
        config = configuration;
        connectorName = connName;
        emitter = new OpenLineageEventEmitter(configuration);
        // TODO instantiate this just one time
        openLineageContext = new OpenLineageContext(
                new OpenLineage(emitter.getProducer()),
                configuration.subset("openlineage.integration", false),
                new OpenLineageJobIdentifier(configuration.getString(CommonConnectorConfig.TOPIC_PREFIX),
                        configuration.getString(CommonConnectorConfig.TOPIC_PREFIX)));
    }

    public static void emit(BaseSourceTask.State state) {

        emit(state, null);
    }

    public static void emit(BaseSourceTask.State state, Table event) {

        if (emitter.isEnabled()) {

            OpenLineage.Job job = new OpenLineageJobCreator(openLineageContext).create();

            List<OpenLineage.InputDataset> inputs = getInputDatasets(event);

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
                    .run(openLineageContext.getOpenLineage().newRun(openLineageContext.getRunUuid(), runFacets))
                    .inputs(inputs)
                    .job(job)
                    .build();

            emitter.emit(startEvent);
        }
    }

    private static List<OpenLineage.InputDataset> getInputDatasets(Table table) {

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
                                        .version(openLineageContext.getOpenLineage().newDatasetVersionDatasetFacet("input-version"))
                                        .build())
                        .build());
    }

    private static String extractNamespacePrefix() {
        return switch (connectorName) {
            case "postgresql" -> "postgres";
            default -> connectorName;
        };
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
