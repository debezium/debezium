/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.mapping;

import io.debezium.relational.Column;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import java.sql.Types;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Orr Ganani
 *
 */
public class OriginalDataTypeTest {

    private final int length = 255;
    private final Column column = Column.editor()
        .name("col")
        .type("VARCHAR")
        .jdbcType(Types.VARCHAR)
        .length(length)
        .create();

    @Test
    public void shouldTruncateStrings() {
        SchemaBuilder schemaBuilder = SchemaBuilder.string();
        new AddOriginalDataType().alterFieldSchema(column, schemaBuilder);
        assertThat(schemaBuilder.parameters().get("originalType")).isEqualTo(String.valueOf(Types.VARCHAR));
        assertThat(schemaBuilder.parameters().get("columnSize")).isEqualTo(String.valueOf(length));
    }
}
