/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class EnumTest {

    @Test
    public void shouldCreateSchemaBuilderFromValues() {
        assertBuilder(Enum.builder(Arrays.asList("a", "b", "c")), "a,b,c");
        assertBuilder(Enum.builder(Arrays.asList("a")), "a");
        assertBuilder(Enum.builder(Collections.EMPTY_LIST), "");
        assertBuilder(Enum.builder((List<String>) null), "");
    }

    @Test
    public void shouldCreateSchemaFromValues() {
        assertSchema(Enum.schema(Arrays.asList("a", "b", "c")), "a,b,c");
        assertSchema(Enum.schema(Arrays.asList("a")), "a");
        assertSchema(Enum.schema(Collections.EMPTY_LIST), "");
        assertSchema(Enum.schema((List<String>) null), "");
    }

    private void assertBuilder(SchemaBuilder builder, String expectedAllowedValues) {
        assertThat(builder).isNotNull();
        assertThat(builder.parameters()).isNotNull();
        assertThat(builder.parameters().get(Enum.VALUES_FIELD)).isEqualTo(expectedAllowedValues);
    }

    private void assertSchema(Schema schema, String expectedAllowedValues) {
        assertThat(schema).isNotNull();
        assertThat(schema.parameters()).isNotNull();
        assertThat(schema.parameters().get(Enum.VALUES_FIELD)).isEqualTo(expectedAllowedValues);
    }
}
