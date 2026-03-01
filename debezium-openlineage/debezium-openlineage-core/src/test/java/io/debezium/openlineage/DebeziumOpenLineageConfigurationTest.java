/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.debezium.config.ConfigurationNames;

public class DebeziumOpenLineageConfigurationTest {

    @Test
    void testFromConfigurationParsesAllFieldsCorrectly() {

        Map<String, String> config = Map.of(
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_ENABLED, "true",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_CONFIG_FILE_PATH, "/etc/debezium/openlineage.yml",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_NAMESPACE, "test-namespace",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_DESCRIPTION, "This is a test job",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_TAGS, "tag1=value1,tag2=value2",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_OWNERS, "owner1=teamA,owner2=teamB");

        DebeziumOpenLineageConfiguration result = DebeziumOpenLineageConfiguration
                .from(new ConnectorContext("test-connector", "a-name", "0", "3.3.0.Final", UUID.randomUUID(), config));

        assertTrue(result.enabled());
        assertEquals("/etc/debezium/openlineage.yml", result.config().path());

        DebeziumOpenLineageConfiguration.Job job = result.job();
        assertEquals("test-namespace", job.namespace());
        assertEquals("This is a test job", job.description());
        assertEquals(Map.of("tag1", "value1", "tag2", "value2"), job.tags());
        assertEquals(Map.of("owner1", "teamA", "owner2", "teamB"), job.owners());
    }

    @Test
    void testFromConfigurationUsesConnectorLogicalNameAsNamespaceFallback() {
        Map<String, String> config = Map.of(
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_ENABLED, "true",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_CONFIG_FILE_PATH, "conf.yml",
                ConfigurationNames.TOPIC_PREFIX_PROPERTY_NAME, "fallback-prefix",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_DESCRIPTION, "Fallback test",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_TAGS, "tag=value",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_OWNERS, "owner=value");

        DebeziumOpenLineageConfiguration result = DebeziumOpenLineageConfiguration
                .from(new ConnectorContext("test-connector", "a-name", "0", "3.3.0.Final", UUID.randomUUID(), config));

        assertEquals("test-connector", result.job().namespace());
    }

    @Test
    void testEmptyTagsAndOwnersAreParsedAsEmptyMaps() {
        Map<String, String> config = Map.of(
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_ENABLED, "false",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_CONFIG_FILE_PATH, "none",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_NAMESPACE, "some-namespace",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_DESCRIPTION, "");

        DebeziumOpenLineageConfiguration result = DebeziumOpenLineageConfiguration
                .from(new ConnectorContext("test-connector", "a-name", "0", "3.3.0.Final", UUID.randomUUID(), config));

        assertEquals("Debezium CDC job for test-connector", result.job().description());
        assertFalse(result.enabled());
        assertTrue(result.job().tags().isEmpty());
        assertTrue(result.job().owners().isEmpty());
    }

    @Test
    void testMalformedTagEntryThrowsException() {
        Map<String, String> config = Map.of(
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_ENABLED, "true",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_CONFIG_FILE_PATH, "file.yml",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_NAMESPACE, "namespace",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_DESCRIPTION, "desc",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_TAGS, "tagOnlyNoEquals",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_JOB_OWNERS, "");

        assertThrows(ArrayIndexOutOfBoundsException.class, () -> {
            DebeziumOpenLineageConfiguration.from(new ConnectorContext("test-connector", "a-name", "0", "3.3.0.Final", UUID.randomUUID(), config));
        });

    }

    @Test
    void testMissingJobDescriptionUsesDefault() {
        Map<String, String> config = Map.of(
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_ENABLED, "true",
                OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_CONFIG_FILE_PATH, "conf.yml");

        DebeziumOpenLineageConfiguration result = DebeziumOpenLineageConfiguration.from(
                new ConnectorContext("test-connector", "a-name", "0", "3.3.0.Final", UUID.randomUUID(), config));

        assertEquals("Debezium CDC job for test-connector", result.job().description());
    }
}
