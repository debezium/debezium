package io.debezium.config;

import io.debezium.doc.FixFor;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FieldTest {

    @Test
    @FixFor("")
    public void shouldHaveDeprecatedAliasesAndDefault() {
        Field field = Field.create("new.field")
                .withDescription("a description")
                .withDeprecatedAliases("deprecated.field")
                .withDefault("default");

        assertThat(field.deprecatedAliases()).isNotEmpty();
        assertThat(field.defaultValue()).isNotNull();
    }

    @Test
    @FixFor("")
    public void shouldHaveDefaultAndDeprecatedAliases() {
        Field field = Field.create("new.field")
                .withDescription("a description")
                .withDefault("default")
                .withDeprecatedAliases("deprecated.field");

        assertThat(field.deprecatedAliases()).isNotEmpty();
        assertThat(field.defaultValue()).isNotNull();
    }
}