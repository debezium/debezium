/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class EnumSetTest {
    @Test
    public void shouldCreateSchemaBuilderFromValues() {
        assertBuilder(EnumSet.builder(Arrays.asList("a", "b", "c")), "a,b,c");
        assertBuilder(EnumSet.builder(Arrays.asList("a")), "a");
        assertBuilder(EnumSet.builder(Collections.EMPTY_LIST), "");
        assertBuilder(EnumSet.builder((List<String>) null), "");
    }

    @Test
    public void shouldCreateSchemaFromValues() {
        assertSchema(EnumSet.schema(Arrays.asList("a", "b", "c")), "a,b,c");
        assertSchema(EnumSet.schema(Arrays.asList("a")), "a");
        assertSchema(EnumSet.schema(Collections.EMPTY_LIST), "");
        assertSchema(EnumSet.schema((List<String>) null), "");
    }

    private void assertBuilder(SchemaBuilder builder, String expectedAllowedValues) {
        assertThat(builder).isNotNull();
        assertThat(builder.parameters()).isNotNull();
        assertThat(builder.parameters().get(EnumSet.VALUES_FIELD)).isEqualTo(expectedAllowedValues);
    }

    private void assertSchema(Schema schema, String expectedAllowedValues) {
        assertThat(schema).isNotNull();
        assertThat(schema.parameters()).isNotNull();
        assertThat(schema.parameters().get(EnumSet.VALUES_FIELD)).isEqualTo(expectedAllowedValues);
    }
}