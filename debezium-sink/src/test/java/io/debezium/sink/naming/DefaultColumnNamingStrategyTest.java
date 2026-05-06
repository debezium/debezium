/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.naming;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DefaultColumnNamingStrategyTest {

    private final DefaultColumnNamingStrategy strategy = new DefaultColumnNamingStrategy();

    @Test
    void shouldReturnFieldNameUnchanged() {
        assertThat(strategy.resolveColumnName("my_field")).isEqualTo("my_field");
    }

    @Test
    void shouldPreserveSpecialCharacters() {
        assertThat(strategy.resolveColumnName("nick_name$")).isEqualTo("nick_name$");
    }

    @Test
    void shouldPreserveCasing() {
        assertThat(strategy.resolveColumnName("MyField")).isEqualTo("MyField");
    }
}
