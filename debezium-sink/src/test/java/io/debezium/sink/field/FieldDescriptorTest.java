/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.field;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

class FieldDescriptorTest {

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldStoreSchemaNameAndKeyFlag() {
        FieldDescriptor field = new FieldDescriptor(Schema.STRING_SCHEMA, "name", false);

        assertThat(field.getSchema()).isEqualTo(Schema.STRING_SCHEMA);
        assertThat(field.getName()).isEqualTo("name");
        assertThat(field.isKey()).isFalse();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldUseFieldNameAsColumnNameByDefault() {
        FieldDescriptor field = new FieldDescriptor(Schema.INT32_SCHEMA, "id", true);

        assertThat(field.getColumnName()).isEqualTo("id");
        assertThat(field.isKey()).isTrue();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldUseSourceColumnNameFromSchemaParameters() {
        Schema schema = SchemaBuilder.string()
                .parameters(Map.of("__debezium.source.column.name", "original_col"))
                .build();

        FieldDescriptor field = new FieldDescriptor(schema, "renamed_col", false);

        assertThat(field.getColumnName()).isEqualTo("original_col");
    }
}
