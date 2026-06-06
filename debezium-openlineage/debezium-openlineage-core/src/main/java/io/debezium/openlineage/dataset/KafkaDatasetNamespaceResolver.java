/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import static io.debezium.openlineage.DebeziumOpenLineageConfiguration.getList;
import static io.debezium.openlineage.OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_DATASET_KAFKA_BOOTSTRAP_SERVER;

import java.util.Map;
import java.util.function.Function;

public class KafkaDatasetNamespaceResolver implements DatasetNamespaceResolver {

    private static final String LIST_SEPARATOR = ",";
    /**
     * Format string for constructing Kafka namespace identifiers according to the OpenLineage specification.
     * <p>
     * The namespace format follows the pattern "kafka://{bootstrap server host}:{port}" where:
     * <ul>
     * <li>bootstrap server host - the hostname or IP address of the Kafka bootstrap server</li>
     * <li>port - the port number on which the Kafka bootstrap server is listening</li>
     * </ul>
     * <p>
     * Example usage: {@code String.format(KAFKA_NAMESPACE_FORMAT, "localhost:9092")}
     * results in "kafka://localhost:9092"
     *
     * @see <a href="https://openlineage.io/docs/spec/naming">OpenLineage Naming Convention</a>
     */
    private static final String KAFKA_NAMESPACE_FORMAT = "kafka://%s";
    private static final String SCHEMA_HISTORY_INTERNAL_KAFKA_BOOTSTRAP_SERVERS_PROPERTY = "schema.history.internal.kafka.bootstrap.servers";

    @Override
    public String resolve(Map<String, String> configuration, String connectorName) {

        String autoDetectedKafkaAddress = getList(configuration, SCHEMA_HISTORY_INTERNAL_KAFKA_BOOTSTRAP_SERVERS_PROPERTY, LIST_SEPARATOR, Function.identity())
                .stream().findFirst()
                .orElse("unknown:unknown");

        String kafkaAddress = configuration.getOrDefault(OPEN_LINEAGE_INTEGRATION_DATASET_KAFKA_BOOTSTRAP_SERVER, autoDetectedKafkaAddress);

        return String.format(KAFKA_NAMESPACE_FORMAT, kafkaAddress);
    }
}
