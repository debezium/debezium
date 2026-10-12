/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.junit.relational.TestRelationalDatabaseConfig;
import io.debezium.relational.TableId;

public class SignalDataCollectionMatchingTest {

    private final CommonConnectorConfig config = new TestRelationalDatabaseConfig(
            Configuration.create()
                    .with(CommonConnectorConfig.TOPIC_PREFIX, "test")
                    .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION, "inventory.debezium_signal")
                    .build(),
            null, null, 1);

    @Test
    @FixFor("debezium/dbz#125")
    public void shouldMatchSignalDataCollectionExactly() {
        assertThat(config.isSignalDataCollection(new TableId("inventory", null, "debezium_signal"))).isTrue();
        assertThat(config.isSignalDataCollection(new TableId("Inventory", null, "Debezium_Signal"))).isFalse();
        assertThat(config.isSignalDataCollection(new TableId("Inventory", null, "Debezium_Signal"), false)).isFalse();
        assertThat(config.isSignalDataCollection(new TableId("inventory", null, "products"), false)).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#125")
    public void shouldMatchSignalDataCollectionIgnoringCaseWhenTableIdentifiersAreCaseInsensitive() {
        assertThat(config.isSignalDataCollection(new TableId("inventory", null, "debezium_signal"), true)).isTrue();
        assertThat(config.isSignalDataCollection(new TableId("Inventory", null, "Debezium_Signal"), true)).isTrue();
        assertThat(config.isSignalDataCollection(new TableId("inventory", null, "products"), true)).isFalse();
        assertThat(config.isSignalDataCollection(new TableId("other", null, "debezium_signal"), true)).isFalse();
    }
}
