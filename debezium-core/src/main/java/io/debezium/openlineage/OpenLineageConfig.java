/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Field;

/**
 * Configuration fields for OpenLineage integration with Debezium.
 *
 * <p>This class defines all configuration options available for enabling and customizing
 * OpenLineage data lineage tracking within Debezium connectors. OpenLineage provides
 * standardized metadata about data pipelines, including job information, data sources,
 * and transformation details.</p>
 *
 * <p>All configuration fields are defined as static {@link Field} instances that can be
 * used to build Debezium connector configurations. These fields are grouped under the
 * ADVANCED configuration group with low importance, as they are optional features.</p>
 *
 * <p>For more information about OpenLineage configuration, see:
 * <a href="https://openlineage.io/docs/client/java/configuration">OpenLineage Java Client Configuration</a></p>
 *
 * @see Field
 * @see ConfigDef
 */
public class OpenLineageConfig {

    /**
     * Configuration field to enable or disable OpenLineage integration.
     *
     * <p>When enabled, Debezium will emit data lineage metadata through the OpenLineage API,
     * providing visibility into data flows and transformations performed by the connector.</p>
     *
     * <p><strong>Configuration key:</strong> {@code openlineage.integration.enabled}<br>
     * <strong>Type:</strong> Boolean<br>
     * <strong>Default:</strong> {@code false}<br>
     * <strong>Importance:</strong> Low</p>
     */
    public static Field OPEN_LINEAGE_INTEGRATION_ENABLED = Field.create("openlineage.integration.enabled")
            .withDisplayName("Enables OpenLineage integration")
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 40))
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Enable Debezium to emit data lineage metadata through OpenLineage API")
            .withDefault(false);

    /**
     * Configuration field specifying the path to the OpenLineage configuration file.
     *
     * <p>This file contains OpenLineage client configuration settings such as transport
     * configuration, backend settings, and other OpenLineage-specific options.</p>
     *
     * <p><strong>Configuration key:</strong> {@code openlineage.integration.config.file.path}<br>
     * <strong>Type:</strong> String<br>
     * <strong>Default:</strong> {@code ./openlineage.yml}<br>
     * <strong>Importance:</strong> Low</p>
     *
     * @see <a href="https://openlineage.io/docs/client/java/configuration">OpenLineage Configuration Documentation</a>
     */
    public static Field OPEN_LINEAGE_INTEGRATION_CONFIG_FILE_PATH = Field.create("openlineage.integration.config.file.path")
            .withDisplayName("Path to OpenLineage file configuration")
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 41))
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("./openlineage.yml")
            .withDescription("Path to OpenLineage file configuration. See https://openlineage.io/docs/client/java/configuration");

    /**
     * Configuration field defining the namespace for the Debezium job in OpenLineage.
     *
     * <p>The namespace is used to organize and categorize jobs within the OpenLineage
     * metadata system. It helps in identifying and grouping related data pipeline jobs.</p>
     *
     * <p><strong>Configuration key:</strong> {@code openlineage.integration.job.namespace}<br>
     * <strong>Type:</strong> String<br>
     * <strong>Default:</strong> None<br>
     * <strong>Importance:</strong> Low</p>
     */
    public static Field OPEN_LINEAGE_INTEGRATION_JOB_NAMESPACE = Field.create("openlineage.integration.job.namespace")
            .withDisplayName("Namespace used for Debezium job")
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 42))
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The job's namespace emitted by Debezium");

    /**
     * Configuration field providing a human-readable description for the Debezium job.
     *
     * <p>This description appears in OpenLineage metadata and helps users understand
     * the purpose and function of the specific Debezium connector job.</p>
     *
     * <p><strong>Configuration key:</strong> {@code openlineage.integration.job.description}<br>
     * <strong>Type:</strong> String<br>
     * <strong>Default:</strong> {@code "Debezium change data capture job"}<br>
     * <strong>Importance:</strong> Low</p>
     */
    public static Field OPEN_LINEAGE_INTEGRATION_JOB_DESCRIPTION = Field.create("openlineage.integration.job.description")
            .withDisplayName("Description used for Debezium job")
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 43))
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The job's description emitted by Debezium")
            .withDefault("Debezium change data capture job");

    /**
     * Configuration field for specifying tags associated with the Debezium job.
     *
     * <p>Tags provide additional metadata and categorization for the job in OpenLineage.
     * They are specified as a comma-separated list of key-value pairs.</p>
     *
     * <p><strong>Configuration key:</strong> {@code openlineage.integration.job.tags}<br>
     * <strong>Type:</strong> List<br>
     * <strong>Format:</strong> Comma-separated key-value pairs (e.g., {@code k1=v1,k2=v2})<br>
     * <strong>Default:</strong> None<br>
     * <strong>Importance:</strong> Low</p>
     *
     * <p><strong>Example:</strong>
     * <pre>{@code
     * openlineage.integration.job.tags=environment=production,team=data-engineering
     * }</pre>
     */
    public static Field OPEN_LINEAGE_INTEGRATION_JOB_TAGS = Field.create("openlineage.integration.job.tags")
            .withDisplayName("Debezium job tags")
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 44))
            .withType(ConfigDef.Type.LIST)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withValidation(Field::isListOfMap)
            .withDescription("The job's tags emitted by Debezium. A comma-separated list of key-value pairs.For example: k1=v1,k2=v2");

    /**
     * Configuration field for specifying the owners of the Debezium job.
     *
     * <p>Owners identify who is responsible for the job and can be used for contact
     * information, governance, and accountability within the OpenLineage metadata system.
     * They are specified as a comma-separated list of key-value pairs.</p>
     *
     * <p><strong>Configuration key:</strong> {@code openlineage.integration.job.owners}<br>
     * <strong>Type:</strong> List<br>
     * <strong>Format:</strong> Comma-separated key-value pairs (e.g., {@code k1=v1,k2=v2})<br>
     * <strong>Default:</strong> None<br>
     * <strong>Importance:</strong> Low</p>
     *
     * <p><strong>Example:</strong>
     * <pre>{@code
     * openlineage.integration.job.owners=email=team@company.com,slack=data-team
     * }</pre>
     */
    public static Field OPEN_LINEAGE_INTEGRATION_JOB_OWNERS = Field.create("openlineage.integration.job.owners")
            .withDisplayName("Debezium job owners")
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 45))
            .withType(ConfigDef.Type.LIST)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withValidation(Field::isListOfMap)
            .withDescription("The job's owners emitted by Debezium. A comma-separated list of key-value pairs.For example: k1=v1,k2=v2");
}
