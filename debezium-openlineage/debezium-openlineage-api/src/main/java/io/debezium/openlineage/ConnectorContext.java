/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import static io.debezium.config.ConfigurationNames.TASK_ID_PROPERTY_NAME;
import static io.debezium.config.ConfigurationNames.TOPIC_PREFIX_PROPERTY_NAME;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.common.DebeziumHeaders;

/**
 * Represents the context information for a Kafka Connect connector, containing
 * essential metadata required for connector operations and identification.
 *
 * <p>This record encapsulates connector identification details including the logical name,
 * connector name, task ID, and configuration. It provides factory methods to create
 * instances from different sources such as configuration objects and message headers.</p>
 *
 * @param connectorLogicalName the logical name of the connector, typically used for identification
 * @param connectorName the actual name of the connector type
 * @param taskId the unique identifier for the connector task, defaults to "0" if not specified
 * @param config the configuration object containing connector settings, may be null when created from headers
 *
 * @author Mario Fiore Vitale
 */
public record ConnectorContext(String connectorLogicalName,
        String connectorName,
        String taskId,
        String version,
        Map<String, String> config) {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorContext.class);

    private static final String KEY_FORMAT = "%s:%s";

    /**
     * Creates a ConnectorContext instance from a configuration object and connector type name.
     *
     * <p>This factory method extracts the topic prefix and task ID from the provided configuration.
     * If no task ID is found in the configuration, it defaults to "0".</p>
     *
     * @param configuration the configuration object containing connector settings
     * @param connectorTypeName the name of the connector type
     * @return a new ConnectorContext instance populated with configuration values
     */
    public static ConnectorContext from(Map<String, String> configuration, String connectorTypeName) {

        return new ConnectorContext(
                configuration.get(TOPIC_PREFIX_PROPERTY_NAME),
                connectorTypeName,
                configuration.getOrDefault(TASK_ID_PROPERTY_NAME, "0"),
                Module.version(),
                configuration);
    }

    /**
     * Creates a ConnectorContext instance from Kafka message headers.
     *
     * <p>This factory method extracts Debezium-specific context headers to populate
     * the connector context. It filters headers that start with the Debezium context
     * header prefix and maps them for easy access. Note that the configuration
     * parameter will be null when using this method.</p>
     *
     * <p><strong>Warning:</strong> This method logs a warning indicating that the
     * OpenLineage SMT cannot retrieve required Debezium headers.</p>
     *
     * @param headers the Kafka message headers containing connector context information
     * @return a new ConnectorContext instance with data extracted from headers,
     *         with config set to null
     */
    public static ConnectorContext from(Headers headers) {

        Map<String, Object> contextHeaders = StreamSupport.stream(headers.spliterator(), false)
                .filter(header -> header.key().startsWith(DebeziumHeaders.DEBEZIUM_CONTEXT_HEADER_PREFIX))
                .collect(Collectors.toMap(Header::key, Header::value));

        if (contextHeaders.isEmpty()) {
            LOGGER.warn("OpenLineage SMT is not able to get required {} headers.", DebeziumHeaders.DEBEZIUM_HEADER_PREFIX);
        }

        return new ConnectorContext(
                (String) contextHeaders.get(DebeziumHeaders.DEBEZIUM_CONNECTOR_LOGICAL_NAME_HEADER),
                (String) contextHeaders.get(DebeziumHeaders.DEBEZIUM_CONNECTOR_NAME_HEADER),
                (String) contextHeaders.get(DebeziumHeaders.DEBEZIUM_TASK_ID_HEADER),
                null,
                null);
    }

    /**
     * Generates a unique emitter key for this connector context.
     *
     * <p>The emitter key is formatted as "{connectorLogicalName}:{taskId}" and can be
     * used to uniquely identify this connector instance for emitting events.</p>
     *
     * @return a formatted string combining the connector logical name and task ID
     */
    public String toEmitterKey() {
        return String.format(KEY_FORMAT, connectorLogicalName, taskId);
    }
}
