/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Map.Entry;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.connect.source.SourceConnector;
import org.junit.Test;

/**
 * Applies basic checks of completeness for the connector-specific configuration metadata.
 */
public abstract class ConfigDefinitionMetadataTest {

    private final ConfigDef config;

    protected ConfigDefinitionMetadataTest(SourceConnector connector) {
        this.config = connector.config();
    }

    @Test
    public void allFieldsShouldHaveDescription() {
        for (Entry<String, ConfigKey> configKey : config.configKeys().entrySet()) {
            assertThat(configKey.getValue().documentation)
                    .describedAs("Description of config key \"" + configKey.getKey() + "\"")
                    .isNotNull()
                    .isNotEmpty();
        }
    }
}
