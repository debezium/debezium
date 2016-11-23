/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.sql.Types;

import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class ColumnEditorTest {

    private ColumnEditor editor;
    private Column column;

    @Before
    public void beforeEach() {
        editor = Column.editor();
        column = null;
    }
    
    protected void createColumnWithAllFieldsSetToNonDefaults() {
        column = editor.name("price")
                .type("NUMBER")
                .jdbcType(Types.DOUBLE)
                .length(5)
                .scale(2)
                .position(4)
                .optional(true)
                .autoIncremented(true)
                .generated(true)
                .create();
    }

    @Test
    public void shouldCreateColumnWithAllFieldsSetToNonDefaults() {
        createColumnWithAllFieldsSetToNonDefaults();
        assertThat(column.name()).isEqualTo("price");
        assertThat(column.typeName()).isEqualTo("NUMBER");
        assertThat(column.jdbcType()).isEqualTo(Types.DOUBLE);
        assertThat(column.length()).isEqualTo(5);
        assertThat(column.scale()).isEqualTo(2);
        assertThat(column.position()).isEqualTo(4);
        assertThat(column.isOptional()).isTrue();
        assertThat(column.isAutoIncremented()).isTrue();
        assertThat(column.isGenerated()).isTrue();
    }

    @Test
    public void shouldCreateColumnWithAllFieldsSetToDefaults() {
        Column column = editor.create();
        assertThat(column.name()).isNull();
        assertThat(column.typeName()).isNull();
        assertThat(column.jdbcType()).isEqualTo(Types.INTEGER);
        assertThat(column.length()).isEqualTo(-1);
        assertThat(column.scale()).isEqualTo(-1);
        assertThat(column.position()).isEqualTo(1);
        assertThat(column.isOptional()).isTrue();
        assertThat(column.isAutoIncremented()).isFalse();
        assertThat(column.isGenerated()).isFalse();
    }

    @Test
    public void shouldHaveToStringThatMatchesColumn() {
        createColumnWithAllFieldsSetToNonDefaults();
        assertThat(editor.toString()).isEqualTo(column.toString());
        assertThat(editor.toString()).isNotEmpty();
    }

    @Test
    public void shouldCompareBasedUponPosition() {
        createColumnWithAllFieldsSetToNonDefaults();
        assertThat(editor.compareTo(column)).isEqualTo(0);
        editor.position(100);
        assertThat(editor.compareTo(column)).isGreaterThan(0);
        editor.position(1);
        assertThat(editor.compareTo(column)).isLessThan(0);
    }

}
