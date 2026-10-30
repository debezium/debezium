/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.schema.FieldNameSelector.FieldNamer;

/**
 * Unit tests for {@link FieldNameSelector}.
 */
public class FieldNameSelectorTest {

    @Test
    @FixFor("debezium/dbz#2982")
    public void shouldReturnSameFieldNameForColumnsWithSameNameAndDifferentAttributes() {
        final FieldNamer<Column> fieldNamer = FieldNameSelector.defaultSelector(SchemaNameAdjuster.create(new FieldNameUnderscoreReplacementFunction()));

        final Column bigintColumn = Column.editor().name("my-col").type("int8").jdbcType(Types.BIGINT).position(1).optional(false).create();
        final Column varcharColumn = Column.editor().name("my-col").type("varchar").jdbcType(Types.VARCHAR).length(255).position(3).optional(true)
                .defaultValueExpression("'new'").create();

        assertThat(fieldNamer.fieldNameFor(bigintColumn)).isEqualTo("my_col");
        assertThat(fieldNamer.fieldNameFor(varcharColumn)).isEqualTo("my_col");
    }

    @Test
    @FixFor("debezium/dbz#2982")
    public void shouldKeepFieldNameCaseOfColumnsDifferingOnlyInCase() {
        final FieldNamer<Column> fieldNamer = FieldNameSelector.defaultSelector(SchemaNameAdjuster.NO_OP);

        final Column upperCaseColumn = Column.editor().name("ID").type("int8").jdbcType(Types.BIGINT).position(1).create();
        final Column lowerCaseColumn = Column.editor().name("id").type("int8").jdbcType(Types.BIGINT).position(1).create();

        assertThat(fieldNamer.fieldNameFor(upperCaseColumn)).isEqualTo("ID");
        assertThat(fieldNamer.fieldNameFor(lowerCaseColumn)).isEqualTo("id");
    }

    @Test
    @FixFor("debezium/dbz#2982")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void shouldNameManyDistinctColumnsWithSameNameQuickly() {
        final FieldNamer<Column> fieldNamer = FieldNameSelector.defaultSelector(SchemaNameAdjuster.NO_OP);

        // Same column name in many tables, each with a table specific default, as for "id bigserial" columns.
        // Caching by Column made each lookup compare against all previously cached same-named columns.
        for (int i = 0; i < 50_000; i++) {
            final Column column = Column.editor().name("id").type("int8").jdbcType(Types.BIGINT).position(1).optional(false)
                    .defaultValueExpression("nextval('t_" + i + "_id_seq'::regclass)").create();
            assertThat(fieldNamer.fieldNameFor(column)).isEqualTo("id");
        }
    }
}
