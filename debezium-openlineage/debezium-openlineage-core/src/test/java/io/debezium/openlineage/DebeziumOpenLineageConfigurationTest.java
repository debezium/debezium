/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

import io.debezium.config.ConfigurationNames;

public class DebeziumOpenLineageConfigurationTest {

    @Test
    public void testFromConfigurationParsesAllFieldsCorrectly() {

        Map<String, String> config = Map.of(
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_ENABLED, "true",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_CONFIG_FILE_PATH, "/etc/debezium/openlineage.yml",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_NAMESPACE, "test-namespace",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_DESCRIPTION, "This is a test job",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_TAGS, "tag1=value1,tag2=value2",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_OWNERS, "owner1=teamA,owner2=teamB");

        DebeziumOpenLineageConfiguration result = DebeziumOpenLineageConfiguration.from(config);

        assertTrue(result.enabled());
        assertEquals("/etc/debezium/openlineage.yml", result.config().path());

        DebeziumOpenLineageConfiguration.Job job = result.job();
        assertEquals("test-namespace", job.namespace());
        assertEquals("This is a test job", job.description());
        assertEquals(Map.of("tag1", "value1", "tag2", "value2"), job.tags());
        assertEquals(Map.of("owner1", "teamA", "owner2", "teamB"), job.owners());
    }

    @Test
    public void testFromConfigurationUsesTopicPrefixAsNamespaceFallback() {
        Map<String, String> config = Map.of(
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_ENABLED, "true",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_CONFIG_FILE_PATH, "conf.yml",
                ConfigurationNames.TOPIC_PREFIX_PROPERTY_NAME, "fallback-prefix",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_DESCRIPTION, "Fallback test",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_TAGS, "tag=value",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_OWNERS, "owner=value");

        DebeziumOpenLineageConfiguration result = DebeziumOpenLineageConfiguration.from(config);

        assertEquals("fallback-prefix", result.job().namespace());
    }

    @Test
    public void testEmptyTagsAndOwnersAreParsedAsEmptyMaps() {
        Map<String, String> config = Map.of(
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_ENABLED, "false",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_CONFIG_FILE_PATH, "none",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_NAMESPACE, "some-namespace",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_DESCRIPTION, "");

        DebeziumOpenLineageConfiguration result = DebeziumOpenLineageConfiguration.from(config);

        assertFalse(result.enabled());
        assertTrue(result.job().tags().isEmpty());
        assertTrue(result.job().owners().isEmpty());
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testMalformedTagEntryThrowsException() {
        Map<String, String> config = Map.of(
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_ENABLED, "true",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_CONFIG_FILE_PATH, "file.yml",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_NAMESPACE, "namespace",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_DESCRIPTION, "desc",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_TAGS, "tagOnlyNoEquals",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_OWNERS, "");

        DebeziumOpenLineageConfiguration.from(config);
    }
}
