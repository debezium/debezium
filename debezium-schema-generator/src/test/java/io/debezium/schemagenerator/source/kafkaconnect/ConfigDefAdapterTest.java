/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.source.kafkaconnect;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

import io.debezium.config.Field;

/**
 * Tests for {@link ConfigDefAdapter}.
 */
class ConfigDefAdapterTest {

    @Test
    void shouldConvertEmptyConfigDef() {

        ConfigDef configDef = new ConfigDef();
        ConfigDefAdapter adapter = new ConfigDefAdapter();

        Field.Set fields = adapter.adapt(configDef);

        assertThat(fields).isNotNull();
        assertThat(fields.asArray()).isEmpty();
    }

    @Test
    void shouldConvertSimpleStringField() {

        ConfigDef configDef = new ConfigDef()
                .define("test.field", ConfigDef.Type.STRING, "default-value",
                        ConfigDef.Importance.HIGH, "Test field documentation");
        ConfigDefAdapter adapter = new ConfigDefAdapter();

        Field.Set fields = adapter.adapt(configDef);

        assertThat(fields.asArray()).hasSize(1);
        Field field = fields.fieldWithName("test.field");
        assertThat(field).isNotNull();
        assertThat(field.name()).isEqualTo("test.field");
        assertThat(field.type()).isEqualTo(ConfigDef.Type.STRING);
        assertThat(field.importance()).isEqualTo(ConfigDef.Importance.HIGH);
        assertThat(field.description()).isEqualTo("Test field documentation");
        assertThat(field.defaultValue()).isEqualTo("default-value");
    }

    @Test
    void shouldConvertBooleanField() {

        ConfigDef configDef = new ConfigDef()
                .define("bool.field", ConfigDef.Type.BOOLEAN, true,
                        ConfigDef.Importance.MEDIUM, "Boolean field");
        ConfigDefAdapter adapter = new ConfigDefAdapter();

        Field.Set fields = adapter.adapt(configDef);

        Field field = fields.fieldWithName("bool.field");
        assertThat(field).isNotNull();
        assertThat(field.type()).isEqualTo(ConfigDef.Type.BOOLEAN);
        assertThat(field.defaultValue()).isEqualTo(true);
    }

    @Test
    void shouldConvertIntField() {

        ConfigDef configDef = new ConfigDef()
                .define("int.field", ConfigDef.Type.INT, 42,
                        ConfigDef.Importance.LOW, "Int field");
        ConfigDefAdapter adapter = new ConfigDefAdapter();

        Field.Set fields = adapter.adapt(configDef);

        Field field = fields.fieldWithName("int.field");
        assertThat(field).isNotNull();
        assertThat(field.type()).isEqualTo(ConfigDef.Type.INT);
        assertThat(field.defaultValue()).isEqualTo(42);
    }

    @Test
    void shouldConvertLongField() {

        ConfigDef configDef = new ConfigDef()
                .define("long.field", ConfigDef.Type.LONG, 1000L,
                        ConfigDef.Importance.HIGH, "Long field");
        ConfigDefAdapter adapter = new ConfigDefAdapter();

        Field.Set fields = adapter.adapt(configDef);

        Field field = fields.fieldWithName("long.field");
        assertThat(field).isNotNull();
        assertThat(field.type()).isEqualTo(ConfigDef.Type.LONG);
        assertThat(field.defaultValue()).isEqualTo(1000L);
    }

    @Test
    void shouldConvertFieldWithDisplayName() {

        ConfigDef configDef = new ConfigDef()
                .define("field.name", ConfigDef.Type.STRING, "default",
                        ConfigDef.Importance.HIGH, "Documentation",
                        "Connection", 1, ConfigDef.Width.MEDIUM, "Display Name");
        ConfigDefAdapter adapter = new ConfigDefAdapter();

        Field.Set fields = adapter.adapt(configDef);

        Field field = fields.fieldWithName("field.name");
        assertThat(field).isNotNull();
        assertThat(field.displayName()).isEqualTo("Display Name");
        assertThat(field.width()).isEqualTo(ConfigDef.Width.MEDIUM);
    }

    @Test
    void shouldConvertMultipleFields() {

        ConfigDef configDef = new ConfigDef()
                .define("field1", ConfigDef.Type.STRING, "default1",
                        ConfigDef.Importance.HIGH, "Field 1")
                .define("field2", ConfigDef.Type.INT, 10,
                        ConfigDef.Importance.MEDIUM, "Field 2")
                .define("field3", ConfigDef.Type.BOOLEAN, false,
                        ConfigDef.Importance.LOW, "Field 3");
        ConfigDefAdapter adapter = new ConfigDefAdapter();

        Field.Set fields = adapter.adapt(configDef);

        assertThat(fields.asArray()).hasSize(3);
        assertThat(fields.fieldWithName("field1")).isNotNull();
        assertThat(fields.fieldWithName("field2")).isNotNull();
        assertThat(fields.fieldWithName("field3")).isNotNull();
    }

    @Test
    void shouldHandleFieldWithoutDefaultValue() {

        ConfigDef configDef = new ConfigDef()
                .define("no.default", ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH, "No default value");
        ConfigDefAdapter adapter = new ConfigDefAdapter();

        Field.Set fields = adapter.adapt(configDef);

        Field field = fields.fieldWithName("no.default");
        assertThat(field).isNotNull();
        assertThat(field.defaultValue()).isNull();
    }

    @Test
    void shouldConvertListField() {

        ConfigDef configDef = new ConfigDef()
                .define("list.field", ConfigDef.Type.LIST, "item1,item2",
                        ConfigDef.Importance.MEDIUM, "List field");
        ConfigDefAdapter adapter = new ConfigDefAdapter();

        Field.Set fields = adapter.adapt(configDef);

        Field field = fields.fieldWithName("list.field");
        assertThat(field).isNotNull();
        assertThat(field.type()).isEqualTo(ConfigDef.Type.LIST);
    }
}
