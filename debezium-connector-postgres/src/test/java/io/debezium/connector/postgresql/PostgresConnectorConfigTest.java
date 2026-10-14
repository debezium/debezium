/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link PostgresConnectorConfig}.
 */
public class PostgresConnectorConfigTest {

    @Test
    @FixFor("debezium/dbz#2868")
    public void shouldAcceptValidUnavailableValuePlaceholder() {
        for (String placeholder : List.of("__debezium_unavailable_value", "hex:", "hex:ff00", "hex:0A1b")) {
            assertThat(placeholderValidationErrors(placeholder)).as(placeholder).isEmpty();
        }

        final var config = new PostgresConnectorConfig(placeholderConfig("hex:ff00"));
        assertThat(config.getUnavailableValuePlaceholder()).containsExactly((byte) 0xff, (byte) 0x00);
    }

    @Test
    @FixFor("debezium/dbz#2868")
    public void shouldRejectInvalidHexUnavailableValuePlaceholder() {
        for (String placeholder : List.of("hex:abc", "hex:zz", "hex:0x01", "hex: ff")) {
            assertThat(placeholderValidationErrors(placeholder)).as(placeholder).hasSize(1);
        }
    }

    private static List<String> placeholderValidationErrors(String placeholder) {
        return placeholderConfig(placeholder)
                .validate(PostgresConnectorConfig.ALL_FIELDS)
                .get(PostgresConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER.name())
                .errorMessages();
    }

    private static Configuration placeholderConfig(String placeholder) {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER, placeholder)
                .build();
    }
}
