/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import static io.debezium.config.ConfigurationDefinition.TOPIC_PREFIX_PROPERTY_NAME;
import static io.debezium.openlineage.OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_CONFIG_FILE_PATH;
import static io.debezium.openlineage.OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_ENABLED;
import static io.debezium.openlineage.OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_DESCRIPTION;
import static io.debezium.openlineage.OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_NAMESPACE;
import static io.debezium.openlineage.OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_OWNERS;
import static io.debezium.openlineage.OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_TAGS;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    public static DebeziumOpenLineageConfiguration from(Map<String, String> configuration) {

        Map<String, String> tags = getList(configuration, OPEN_LINEAGE_INTEGRATION_JOB_TAGS, LIST_SEPARATOR, pair -> pair.split(KEY_VALUE_SEPARATOR))
                .stream()
                .collect(Collectors.toMap(pair -> pair[0].trim(), pair -> pair[1].trim()));

        Map<String, String> owners = getList(configuration, OPEN_LINEAGE_INTEGRATION_JOB_OWNERS, LIST_SEPARATOR, pair -> pair.split(KEY_VALUE_SEPARATOR))
                .stream()
                .collect(Collectors.toMap(pair -> pair[0].trim(), pair -> pair[1].trim()));

        return new DebeziumOpenLineageConfiguration(Boolean.parseBoolean(configuration.get(OPEN_LINEAGE_INTEGRATION_ENABLED)),
                new Config(configuration.get(OPEN_LINEAGE_INTEGRATION_CONFIG_FILE_PATH)),
                new Job(
                        configuration.getOrDefault(OPEN_LINEAGE_INTEGRATION_JOB_NAMESPACE, configuration.get(TOPIC_PREFIX_PROPERTY_NAME)),
                        configuration.get(OPEN_LINEAGE_INTEGRATION_JOB_DESCRIPTION),
                        tags,
                        owners));
    }

    public static <T> List<T> getList(Map<String, String> configuration, String key, String separator, Function<String, T> converter) {
        var value = configuration.get(key);
        return value == null ? List.of()
                : Arrays.stream(value.split(Pattern.quote(separator)))
                        .map(String::trim)
                        .map(converter)
                        .collect(Collectors.toList());
    }
}
