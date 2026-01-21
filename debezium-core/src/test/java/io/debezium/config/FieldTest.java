/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

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

    @Test
    public void shouldReturnDependantFieldsBasedOnValue() {

        Field field = Field.create("auth_type")
                .withDescription("Authorization type")
                .withDefault("NONE")
                .withAllowedValues(Set.of("NONE", "BASIC", "SSL"))
                .withDependents("BASIC", List.of("username", "password"))
                .withDependents("SSL", List.of("ssl.protocol"));

        assertThat(field.dependents("BASIC")).containsAll(List.of("username", "password"));
        assertThat(field.dependents("SSL")).containsAll(List.of("ssl.protocol"));
    }

    @Test
    public void aChildFiledShouldHaveARecommenderBasedOnParentValue() {

        ConfigDef configDef = Field.group(new ConfigDef(), "auth",
                Field.create("auth_type")
                        .withDescription("Authorization type")
                        .withDefault("NONE")
                        .withAllowedValues(Set.of("NONE", "BASIC", "SSL"))
                        .withDependents("BASIC", List.of("username", "password"))
                        .withDependents("SSL", List.of("ssl.protocol")),
                Field.create("username")
                        .withDescription("Username for basic authentication"));

        ConfigDef.ConfigKey child = configDef.configKeys().get("username");
        assertThat(child.recommender).isNotNull();
        assertThat(child.recommender.visible(child.name, Map.of("auth_type", "BASIC"))).isTrue();

    }
}
