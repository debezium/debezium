/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.naming;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

class DefaultColumnNamingStrategyTest {

    private final DefaultColumnNamingStrategy strategy = new DefaultColumnNamingStrategy();

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnFieldNameUnchanged() {
        assertThat(strategy.resolveColumnName("my_field")).isEqualTo("my_field");
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldPreserveSpecialCharacters() {
        assertThat(strategy.resolveColumnName("nick_name$")).isEqualTo("nick_name$");
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldPreserveCasing() {
        assertThat(strategy.resolveColumnName("MyField")).isEqualTo("MyField");
    }
}
