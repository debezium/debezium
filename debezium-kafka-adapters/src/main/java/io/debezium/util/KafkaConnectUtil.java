/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.runtime.AbstractHerder;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;

/**
 * An auxiliary class that exposes various Kafka Connect APIs.
 * This should be removed once we make Kafka dependency completely optional.
 *
 * @author Debezium Authors
 */
public class KafkaConnectUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectUtil.class);

    /**
     * Validates provided configuration of the Kafka Connect connector and returns its configuration if it's a valid config.
     *
     * @param connectorConfig Debezium connector {@link Configuration}
     * @param connector Kafka Connect {@link SourceConnector}.
     * @param connectorClassName Class name of Kafka Connect {@link SourceConnector}.
     * @return {@link Map <String, String>} with connector configuration.
     */
    public static Map<String, String> validateAndGetConnectorConfig(final Configuration connectorConfig, final SourceConnector connector,
                                                                    final String connectorClassName) {
        LOGGER.debug("Validating provided connector configuration.");
        final Map<String, String> connectorConfigMap = connectorConfig.asMap();
        final Config validatedConnectorConfig = connector.validate(connectorConfigMap);
        final ConfigInfos configInfos = AbstractHerder.generateResult(connectorClassName, Collections.emptyMap(), validatedConnectorConfig.configValues(),
                connector.config().groups());
        if (configInfos.errorCount() > 0) {
            // TODO Remove the reflection when minimum Kafka version is 4.2. Reflection is necessary to keep
            // with older Kafka versions
            @SuppressWarnings("unchecked")
            final String errors = ((List<ConfigInfo>) Reflections.invokeMethodWithFallbackName(configInfos, "configs", "values", List.class)).stream()
                    .flatMap(v -> v.configValue().errors().stream())
                    .collect(Collectors.joining(" "));
            throw new DebeziumException("Connector configuration is not valid. " + errors);
        }
        LOGGER.debug("Connector configuration is valid.");
        return connectorConfigMap;
    }

}
