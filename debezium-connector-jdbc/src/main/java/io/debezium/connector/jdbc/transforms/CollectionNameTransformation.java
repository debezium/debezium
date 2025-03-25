/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.jdbc.Module;
import io.debezium.connector.jdbc.util.NamingStyle;
import io.debezium.connector.jdbc.util.NamingStyleUtils;

/**
 * A Kafka Connect SMT (Single Message Transformation) that modifies collection (table) names
 * in record topics. This transformation can apply a prefix, suffix, and enforce a specific
 * naming style (such as snake_case, camelCase, etc.) to ensure consistent naming conventions
 * across all sink collections.
 * <p>
 * Configuration options:
 * <ul>
 *   <li>{@code collection.naming.prefix}: Optional prefix to add to collection names</li>
 *   <li>{@code collection.naming.suffix}: Optional suffix to add to collection names</li>
 *   <li>{@code collection.naming.style}: The naming style to apply (default, UPPER_CASE,
 *       lower_case, snake_case, camelCase)</li>
 * </ul>
 *
 * @author Gustavo Lira
 * @param <R> The record type
 */
public class CollectionNameTransformation<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(CollectionNameTransformation.class);

    private static final Field PREFIX = Field.create("collection.naming.prefix")
            .withDisplayName("Collection Name Prefix")
            .withType(ConfigDef.Type.STRING)
            .withDefault("")
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Optional prefix to add to collection names.");

    private static final Field SUFFIX = Field.create("collection.naming.suffix")
            .withDisplayName("Collection Name Suffix")
            .withType(ConfigDef.Type.STRING)
            .withDefault("")
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Optional suffix to add to collection names.");

    private static final Field NAMING_STYLE = Field.create("collection.naming.style")
            .withDisplayName("Collection Naming Style")
            .withType(ConfigDef.Type.STRING)
            .withDefault("default")
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The style of collection naming: UPPER_CASE, lower_case, snake_case, camelCase, kebab-case.");

    private String prefix;
    private String suffix;
    private NamingStyle namingStyle;

    /**
     * Configures this transformation with the given key-value pairs.
     *
     * @param configs map of configuration parameters
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        this.prefix = config.getString(PREFIX);
        this.suffix = config.getString(SUFFIX);
        this.namingStyle = NamingStyle.from(config.getString(NAMING_STYLE));

        LOGGER.info("Configured with prefix='{}', suffix='{}', naming style='{}'",
                prefix, suffix, namingStyle.getValue());
    }

    /**
     * Applies the transformation to a record, modifying its topic name according
     * to the configured naming conventions.
     *
     * @param record the record to transform
     * @return a new record with the transformed topic name
     */
    @Override
    public R apply(final R record) {
        if (record.topic() == null) {
            LOGGER.debug("Record has null topic, skipping transformation");
            return record;
        }

        // Extract the original topic name
        String originalTopic = record.topic();

        // Apply naming style transformation
        String transformedTopic = transformTopicName(originalTopic);

        LOGGER.debug("Transformed topic '{}' to '{}'", originalTopic, transformedTopic);

        // Create a new record with the transformed topic name
        return record.newRecord(
                transformedTopic,
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp());
    }

    /**
     * Transforms a topic name by applying the configured naming style, prefix, and suffix.
     *
     * @param originalTopic the original topic name
     * @return the transformed topic name
     */
    private String transformTopicName(String originalTopic) {
        // First apply the naming style to the original name
        String transformedName = NamingStyleUtils.applyNamingStyle(originalTopic, namingStyle);

        // Add prefix and suffix
        transformedName = prefix + transformedName + suffix;

        return transformedName;
    }

    /**
     * Returns the configuration definition for this transformation.
     *
     * @return configuration definition
     */
    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(PREFIX.name(), ConfigDef.Type.STRING, PREFIX.defaultValue(), ConfigDef.Importance.LOW, PREFIX.description())
                .define(SUFFIX.name(), ConfigDef.Type.STRING, SUFFIX.defaultValue(), ConfigDef.Importance.LOW, SUFFIX.description())
                .define(NAMING_STYLE.name(), ConfigDef.Type.STRING, NAMING_STYLE.defaultValue(), ConfigDef.Importance.LOW, NAMING_STYLE.description());
    }

    /**
     * Closes this transformation and releases any resources.
     */
    @Override
    public void close() {
        // No resources to release
    }

    /**
     * Returns the version of this transformation.
     *
     * @return the version string
     */
    @Override
    public String version() {
        return Module.version();
    }
}