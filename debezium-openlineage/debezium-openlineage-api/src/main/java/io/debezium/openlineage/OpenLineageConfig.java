/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

/**
 * Configuration fields for OpenLineage integration with Debezium.
 *
 * <p>This class defines all configuration options available for enabling and customizing
 * OpenLineage data lineage tracking within Debezium connectors. OpenLineage provides
 * standardized metadata about data pipelines, including job information, data sources,
 * and transformation details.</p>
 *
 */
public class OpenLineageConfig {

    public static final String OPEN_LINEAGE_INTEGRATION_ENABLED = "openlineage.integration.enabled";
    public static final String OPEN_LINEAGE_INTEGRATION_CONFIG_FILE_PATH = "openlineage.integration.config.file.path";
    public static final String OPEN_LINEAGE_INTEGRATION_JOB_NAMESPACE = "openlineage.integration.job.namespace";
    public static final String OPEN_LINEAGE_INTEGRATION_JOB_DESCRIPTION = "openlineage.integration.job.description";
    public static final String OPEN_LINEAGE_INTEGRATION_JOB_TAGS = "openlineage.integration.job.tags";
    public static final String OPEN_LINEAGE_INTEGRATION_JOB_OWNERS = "openlineage.integration.job.owners";
}
