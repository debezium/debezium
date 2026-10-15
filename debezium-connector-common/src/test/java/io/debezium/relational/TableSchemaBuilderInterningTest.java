/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.Properties;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.util.Interner;
import io.debezium.util.Interner.Mode;

public class TableSchemaBuilderInterningTest {

    private final Column name = Column.editor().name("name").type("VARCHAR").jdbcType(Types.VARCHAR).length(10).optional(true).create();

    @AfterEach
    public void resetInterner() {
        Interner.clear();
        Interner.activate(Mode.OFF);
    }

    @Test

    @FixFor("debezium/dbz#2874")
    public void shouldShareColumnSchemasAndConvertersAcrossTables() {
        Interner.activate(Mode.ON);
        final TableSchemaBuilder builder = builder(new JdbcValueConverters());
        final TableSchema first = builder.create(naming(), table("t1"), null, null, null);
        final TableSchema second = builder.create(naming(), table("t2"), null, null, null);

        assertThat(first.valueSchema()).isNotSameAs(second.valueSchema());
        assertThat(first.valueSchema().field("name").schema()).isSameAs(second.valueSchema().field("name").schema());

        final Field field = first.valueSchema().field("name");
        final ValueConverter converter = builder.createValueConverterFor(first.id(), name, field);
        assertThat(builder.createValueConverterFor(second.id(), name, field)).isSameAs(converter);
        assertThat(converter.convert("a")).isEqualTo("a");
    }

    @Test

    @FixFor("debezium/dbz#2874")
    public void shouldNotShareConvertersAcrossValueConverterProviders() {
        Interner.activate(Mode.ON);
        final TableSchema schema = builder(new JdbcValueConverters()).create(naming(), table("t1"), null, null, null);
        final Field field = schema.valueSchema().field("name");

        assertThat(builder(new JdbcValueConverters()).createValueConverterFor(schema.id(), name, field))
                .isNotSameAs(builder(new JdbcValueConverters()).createValueConverterFor(schema.id(), name, field));
    }

    @Test

    @FixFor("debezium/dbz#2874")
    public void shouldNotShareWithoutInterning() {
        final TableSchemaBuilder builder = builder(new JdbcValueConverters());
        final TableSchema first = builder.create(naming(), table("t1"), null, null, null);
        final TableSchema second = builder.create(naming(), table("t2"), null, null, null);

        assertThat(first.valueSchema().field("name").schema()).isEqualTo(second.valueSchema().field("name").schema());
        assertThat(first.valueSchema().field("name").schema()).isNotSameAs(second.valueSchema().field("name").schema());
        final Field field = first.valueSchema().field("name");
        assertThat(builder.createValueConverterFor(first.id(), name, field)).isNotSameAs(builder.createValueConverterFor(second.id(), name, field));
        assertThat(builder.createValueConverterFor(first.id(), name, field).getClass().getSimpleName()).isNotEqualTo("SharedConverter");
    }

    private Table table(String tableName) {
        return Table.editor().tableId(new TableId("db", null, tableName)).addColumn(name).create();
    }

    private static SchemaTopicNamingStrategy naming() {
        final Properties properties = new Properties();
        properties.put("topic.prefix", "test");
        return new SchemaTopicNamingStrategy(properties, false);
    }

    private static TableSchemaBuilder builder(JdbcValueConverters converters) {
        return new TableSchemaBuilder(converters, null, SchemaNameAdjuster.NO_OP, new CustomConverterRegistry(null),
                SchemaBuilder.struct().build(), FieldNameSelector.defaultSelector(SchemaNameAdjuster.NO_OP), false,
                EventConvertingFailureHandlingMode.FAIL);
    }
}
