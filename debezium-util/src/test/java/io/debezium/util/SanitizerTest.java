/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;

import javax.management.ObjectName;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

public class SanitizerTest {

    @Test
    @FixFor("debezium/dbz#2555")
    public void shouldLeaveJmxSafeValuesUnquoted() {
        assertThat(Sanitizer.jmxSanitize("my-connector.0")).isEqualTo("my-connector.0");
        assertThat(Sanitizer.jmxSanitize("a b_c-1.2%")).isEqualTo("a b_c-1.2%");
        assertThat(Sanitizer.jmxSanitize("")).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2555")
    public void shouldQuoteValuesWithJmxUnsafeCharacters() {
        assertThat(Sanitizer.jmxSanitize("has:colon")).isEqualTo(ObjectName.quote("has:colon"));
        assertThat(Sanitizer.jmxSanitize("a*b,c=d")).isEqualTo(ObjectName.quote("a*b,c=d"));
    }
}
