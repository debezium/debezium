/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

import io.debezium.config.Field;

public class HistorizedRelationalDatabaseConnectorConfigTest {

    /**
     * The {@code schema.history.internal} field defaults to the Kafka schema history implementation, which lives in
     * an optional module. When the field was declared as {@link ConfigDef.Type#CLASS}, building the configuration
     * definition eagerly loaded that default class, so any environment that does not ship the Kafka storage module on
     * the classpath (for example the embedded engine) failed with a {@link org.apache.kafka.common.config.ConfigException}.
     */
    @Test
    @io.debezium.doc.FixFor("debezium/dbz#870")
    public void schemaHistoryFieldShouldNotEagerlyLoadDefaultClass() {
        // The Kafka schema history class is not on this module's test classpath, mirroring an embedded-engine setup.
        final ConfigDef configDef = new ConfigDef();

        assertThatCode(() -> Field.group(configDef, "history", HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY))
                .doesNotThrowAnyException();

        // The default is still advertised so environments that do ship the Kafka storage module keep working.
        assertThat(configDef.configKeys().get(HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY.name()).defaultValue)
                .isEqualTo(HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY.defaultValue());
    }
}
