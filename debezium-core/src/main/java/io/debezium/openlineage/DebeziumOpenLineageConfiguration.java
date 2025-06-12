/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import static io.debezium.openlineage.OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_CONFIG_FILE_PATH;
import static io.debezium.openlineage.OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_ENABLED;
import static io.debezium.openlineage.OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_DESCRIPTION;
import static io.debezium.openlineage.OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_NAMESPACE;
import static io.debezium.openlineage.OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_OWNERS;
import static io.debezium.openlineage.OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_TAGS;

import java.util.Map;
import java.util.stream.Collectors;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;

/**
 * A commodity class that maintain the configuration of OpenLineage integration
 *
 * @author Mario Fiore Vitale
 */
public record DebeziumOpenLineageConfiguration(boolean enabled, Config config, Job job) {

    private static final String KEY_VALUE_SEPARATOR = "=";
    private static final String LIST_SEPARATOR = ",";

    public record Config(String path) {
    }

    public record Job(String namespace, String description, Map<String, String> tags, Map<String, String> owners) {
    }

    public static DebeziumOpenLineageConfiguration from(Configuration configuration) {

        Map<String, String> tags = configuration.getList(OPEN_LINEAGE_INTEGRATION_JOB_TAGS, LIST_SEPARATOR, pair -> pair.split(KEY_VALUE_SEPARATOR))
                .stream()
                .collect(Collectors.toMap(pair -> pair[0].trim(), pair -> pair[1].trim()));

        Map<String, String> owners = configuration.getList(OPEN_LINEAGE_INTEGRATION_JOB_OWNERS, LIST_SEPARATOR, pair -> pair.split(KEY_VALUE_SEPARATOR))
                .stream()
                .collect(Collectors.toMap(pair -> pair[0].trim(), pair -> pair[1].trim()));

        return new DebeziumOpenLineageConfiguration(configuration.getBoolean(OPEN_LINEAGE_INTEGRATION_ENABLED),
                new Config(configuration.getString(OPEN_LINEAGE_INTEGRATION_CONFIG_FILE_PATH)),
                new Job(
                        configuration.getString(OPEN_LINEAGE_INTEGRATION_JOB_NAMESPACE, configuration.getString(CommonConnectorConfig.TOPIC_PREFIX)),
                        configuration.getString(OPEN_LINEAGE_INTEGRATION_JOB_DESCRIPTION),
                        tags,
                        owners));
    }
}
