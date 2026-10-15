/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link ConnectorConfigValidationHelper}.
 */
public class ConnectorConfigValidationHelperTest {

    private static final Field PLACEHOLDER = Field.create("test.placeholder");

    @Test
    @FixFor("debezium/dbz#2868")
    public void shouldAcceptValidOrNonPrefixedHexValues() {
        for (String value : List.of("__debezium_unavailable_value", "hex:", "hex:ff00", "hex:0A1b")) {
            assertThat(validateHexPrefixedValue(value)).as(value).isEmpty();
        }
    }

    @Test
    @FixFor("debezium/dbz#2868")
    public void shouldRejectInvalidHexPrefixedValues() {
        for (String value : List.of("hex:abc", "hex:zz", "hex:0x01", "hex: ff")) {
            assertThat(validateHexPrefixedValue(value)).as(value).hasSize(1);
        }
    }

    private static List<String> validateHexPrefixedValue(String value) {
        final Configuration config = Configuration.create().with(PLACEHOLDER, value).build();
        final List<String> problems = new ArrayList<>();
        final int errors = ConnectorConfigValidationHelper.validateHexPrefixedValue(config, PLACEHOLDER, (field, v, problem) -> problems.add(problem));
        assertThat(errors).isEqualTo(problems.size());
        return problems;
    }
}
