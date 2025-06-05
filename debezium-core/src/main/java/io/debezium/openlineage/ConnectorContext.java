/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
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
public record ConnectorContext(String connectorLogicalName, String connectorName, String taskId, Configuration config) {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorContext.class);

    private static final String KEY_FORMAT = "%s:%s";

    /** Configuration key for the topic prefix setting */
    public static final String TOPIC_PREFIX = "topic.prefix";

    /** Configuration key for the connector task ID */
    public static final String CONNECTOR_TASK_ID = "task.id";

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
    public static ConnectorContext from(Configuration configuration, String connectorTypeName) {

        return new ConnectorContext(
                configuration.getString(TOPIC_PREFIX),
                connectorTypeName,
                configuration.getString(CONNECTOR_TASK_ID, "0"),
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