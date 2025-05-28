/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import static io.debezium.config.CommonConnectorConfig.OPENLINEAGE_INTEGRATION_CONFIG_FILE_PATH;
import static io.debezium.config.CommonConnectorConfig.OPENLINEAGE_INTEGRATION_ENABLED;
import static io.debezium.config.CommonConnectorConfig.OPENLINEAGE_INTEGRATION_JOB_DESCRIPTION;
import static io.debezium.config.CommonConnectorConfig.OPENLINEAGE_INTEGRATION_JOB_NAMESPACE;
import static io.debezium.config.CommonConnectorConfig.OPENLINEAGE_INTEGRATION_JOB_OWNERS;
import static io.debezium.config.CommonConnectorConfig.OPENLINEAGE_INTEGRATION_JOB_TAGS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;

public class DebeziumOpenLineageConfigurationTest {

    @Test
    public void testFromConfigurationParsesAllFieldsCorrectly() {
        Configuration config = Configuration.create()
                .with(OPENLINEAGE_INTEGRATION_ENABLED, true)
                .with(OPENLINEAGE_INTEGRATION_CONFIG_FILE_PATH, "/etc/debezium/openlineage.yml")
                .with(OPENLINEAGE_INTEGRATION_JOB_NAMESPACE, "test-namespace")
                .with(OPENLINEAGE_INTEGRATION_JOB_DESCRIPTION, "This is a test job")
                .with(OPENLINEAGE_INTEGRATION_JOB_TAGS, "tag1=value1,tag2=value2")
                .with(OPENLINEAGE_INTEGRATION_JOB_OWNERS, "owner1=teamA,owner2=teamB")
                .build();

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
        Configuration config = Configuration.create()
                .with(OPENLINEAGE_INTEGRATION_ENABLED, true)
                .with(OPENLINEAGE_INTEGRATION_CONFIG_FILE_PATH, "conf.yml")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "fallback-prefix")
                .with(OPENLINEAGE_INTEGRATION_JOB_DESCRIPTION, "Fallback test")
                .with(OPENLINEAGE_INTEGRATION_JOB_TAGS, "tag=value")
                .with(OPENLINEAGE_INTEGRATION_JOB_OWNERS, "owner=value")
                .build();

        DebeziumOpenLineageConfiguration result = DebeziumOpenLineageConfiguration.from(config);

        assertEquals("fallback-prefix", result.job().namespace());
    }

    @Test
    public void testEmptyTagsAndOwnersAreParsedAsEmptyMaps() {
        Configuration config = Configuration.create()
                .with(OPENLINEAGE_INTEGRATION_ENABLED, false)
                .with(OPENLINEAGE_INTEGRATION_CONFIG_FILE_PATH, "none")
                .with(OPENLINEAGE_INTEGRATION_JOB_NAMESPACE, "some-namespace")
                .with(OPENLINEAGE_INTEGRATION_JOB_DESCRIPTION, "")
                .build();

        DebeziumOpenLineageConfiguration result = DebeziumOpenLineageConfiguration.from(config);

        assertFalse(result.enabled());
        assertTrue(result.job().tags().isEmpty());
        assertTrue(result.job().owners().isEmpty());
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testMalformedTagEntryThrowsException() {
        Configuration config = Configuration.create()
                .with(OPENLINEAGE_INTEGRATION_ENABLED, true)
                .with(OPENLINEAGE_INTEGRATION_CONFIG_FILE_PATH, "file.yml")
                .with(OPENLINEAGE_INTEGRATION_JOB_NAMESPACE, "namespace")
                .with(OPENLINEAGE_INTEGRATION_JOB_DESCRIPTION, "desc")
                .with(OPENLINEAGE_INTEGRATION_JOB_TAGS, "tagOnlyNoEquals") // malformed
                .with(OPENLINEAGE_INTEGRATION_JOB_OWNERS, "")
                .build();

        DebeziumOpenLineageConfiguration.from(config);
    }
}
