/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.doc.FixFor;

public class FieldTest {

    @Test
    @FixFor("DBZ-8832")
    public void shouldHaveDeprecatedAliasesAndDefault() {
        Field field = Field.create("new.field")
                .withDescription("a description")
                .withDeprecatedAliases("deprecated.field")
                .withDefault("default");

        assertThat(field.deprecatedAliases()).isNotEmpty();
        assertThat(field.defaultValue()).isNotNull();
    }

    @Test
    @FixFor("DBZ-8832")
    public void shouldHaveDefaultAndDeprecatedAliases() {
        Field field = Field.create("new.field")
                .withDescription("a description")
                .withDefault("default")
                .withDeprecatedAliases("deprecated.field");

        assertThat(field.deprecatedAliases()).isNotEmpty();
        assertThat(field.defaultValue()).isNotNull();
    }
}
