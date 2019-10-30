/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.mapping;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Types;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import io.debezium.doc.FixFor;
import io.debezium.relational.Column;

/**
 * @author Orr Ganani
 */
public class PropagateSourceTypeToSchemaParameterTest {

    @Test
    @FixFor("DBZ-644")
    public void shouldAddTypeInformation() {
        int length = 255;
        Column column = Column.editor()
                .name("col")
                .type("VARCHAR")
                .jdbcType(Types.VARCHAR)
                .length(length)
                .create();

        SchemaBuilder schemaBuilder = SchemaBuilder.string();
        new PropagateSourceTypeToSchemaParameter().alterFieldSchema(column, schemaBuilder);

        assertThat(schemaBuilder.parameters().get("__debezium.source.column.type")).isEqualTo("VARCHAR");
        assertThat(schemaBuilder.parameters().get("__debezium.source.column.length")).isEqualTo(String.valueOf(length));
    }

    @Test
    @FixFor("DBZ-1073")
    public void shouldAddColumnScale() {
        int length = 5;
        int scale = 2;
        Column column = Column.editor()
                .name("col")
                .type("NUMERIC")
                .jdbcType(Types.NUMERIC)
                .length(length)
                .scale(scale)
                .create();

        SchemaBuilder schemaBuilder = SchemaBuilder.string();
        new PropagateSourceTypeToSchemaParameter().alterFieldSchema(column, schemaBuilder);

        assertThat(schemaBuilder.parameters().get("__debezium.source.column.type")).isEqualTo("NUMERIC");
        assertThat(schemaBuilder.parameters().get("__debezium.source.column.length")).isEqualTo(String.valueOf(length));
        assertThat(schemaBuilder.parameters().get("__debezium.source.column.scale")).isEqualTo(String.valueOf(scale));
    }
}
