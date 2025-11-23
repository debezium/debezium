/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.facets;

import static io.debezium.config.CommonConfigurationPatterns.PASSWORD_PATTERN;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.debezium.openlineage.OpenLineageConfig;
import io.openlineage.client.OpenLineage;

public class DebeziumConfigFacet implements OpenLineage.RunFacet {

    public static final String FACET_KEY_NAME = "debezium_config";

    /**
     * Format string for configuration lines in key-value format.
     *
     * <p>This template is used with {@link String#format(String, Object...)} to create
     * configuration entries where the first placeholder represents the configuration name
     * and the second placeholder represents the configuration value.</p>
     *
     * <p>Example usage:
     * <pre>{@code
     * String configLine = String.format(CONFIG_LINE_FORMAT, "database.host", "localhost");
     * // Results in: "database.host=localhost"
     * }</pre>
     *
     * @see String#format(String, Object...)
     */
    private static final String CONFIG_LINE_FORMAT = "%s=%s";

    private static final String MASK_VALUE = "********";

    private final URI producer;
    private final List<String> configs;

    public DebeziumConfigFacet(URI producer, Map<String, String> configurations) {
        this.producer = producer;

        Map<String, String> maskedConfig = maskConfiguration(configurations);
        this.configs = maskedConfig.entrySet().stream()
                .map(e -> String.format(CONFIG_LINE_FORMAT, e.getKey(), e.getValue()))
                .toList();
    }

    private Map<String, String> maskConfiguration(Map<String, String> configurations) {
        String customPattern = configurations.get(OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_SANITIZE_PATTERN);

        Pattern sensitivePattern;
        if (customPattern != null && !customPattern.trim().isEmpty()) {
            String combinedPattern = PASSWORD_PATTERN.pattern() + "|" + customPattern;
            sensitivePattern = Pattern.compile(combinedPattern, Pattern.CASE_INSENSITIVE);
        }
        else {
            sensitivePattern = PASSWORD_PATTERN;
        }

        return configurations.entrySet()
                .stream().collect(Collectors.toMap(Map.Entry::getKey,
                        e -> sensitivePattern.matcher(e.getKey()).matches() ? MASK_VALUE : e.getValue()));
    }

    @Override
    public URI get_producer() {
        return producer;
    }

    @Override
    public URI get_schemaURL() {
        return URI.create("https://github.com/debezium/debezium/tree/main/debezium-openlineage/src/main/java/io/debezium/openlineage/facets/spec/DebeziumRunFacet.json");
    }

    public List<String> getConfigs() {
        return configs;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return Map.of();
    }
}
