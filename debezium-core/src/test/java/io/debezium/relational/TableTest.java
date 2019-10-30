/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Types;

import org.junit.Before;
import org.junit.Test;

public class TableTest {

    private final TableId id = new TableId("catalog", "schema", "table");
    private Table table;
    private Column c1;
    private Column c2;
    private Column c3;
    private Column c4;

    @Before
    public void beforeEach() {
        table = Table.editor()
                .tableId(id)
                .addColumns(Column.editor().name("C1")
                        .type("VARCHAR").jdbcType(Types.VARCHAR).length(10)
                        .generated(true)
                        .optional(false)
                        .create(),
                        Column.editor().name("C2")
                                .type("NUMBER").jdbcType(Types.NUMERIC).length(5)
                                .optional(false)
                                .create(),
                        Column.editor().name("C3")
                                .type("DATE").jdbcType(Types.DATE).length(4)
                                .optional(true)
                                .create(),
                        Column.editor().name("C4")
                                .type("COUNTER").jdbcType(Types.INTEGER)
                                .autoIncremented(true)
                                .optional(true)
                                .create())
                .setPrimaryKeyNames("C1", "C2")
                .create();
        c1 = table.columnWithName("C1");
        c2 = table.columnWithName("C2");
        c3 = table.columnWithName("C3");
        c4 = table.columnWithName("C4");
    }

    @Test
    public void checkPreconditions() {
        assertThat(c1).isNotNull();
        assertThat(c2).isNotNull();
        assertThat(c3).isNotNull();
        assertThat(c4).isNotNull();
    }

    @Test
    public void shouldHaveColumnsWithNames() {
        assertThat(c1.name()).isEqualTo("C1");
        assertThat(c2.name()).isEqualTo("C2");
        assertThat(c3.name()).isEqualTo("C3");
        assertThat(c4.name()).isEqualTo("C4");
    }

    @Test
    public void shouldHaveColumnsWithProperPositions() {
        assertThat(c1.position()).isEqualTo(1);
        assertThat(c2.position()).isEqualTo(2);
        assertThat(c3.position()).isEqualTo(3);
        assertThat(c4.position()).isEqualTo(4);
    }

    @Test
    public void shouldHaveTableId() {
        assertThat(table.id()).isEqualTo(id);
    }

    @Test
    public void shouldHaveColumns() {
        assertThat(table.retrieveColumnNames()).containsExactly("C1", "C2", "C3", "C4");
        assertThat(table.columns()).containsExactly(c1, c2, c3, c4);
    }

    @Test
    public void shouldFindColumnsByNameWithExactCase() {
        assertThat(table.columnWithName("C1")).isSameAs(c1);
        assertThat(table.columnWithName("C2")).isSameAs(c2);
        assertThat(table.columnWithName("C3")).isSameAs(c3);
        assertThat(table.columnWithName("C4")).isSameAs(c4);
    }

    @Test
    public void shouldFindColumnsByNameWithWrongCase() {
        assertThat(table.columnWithName("c1")).isSameAs(c1);
        assertThat(table.columnWithName("c2")).isSameAs(c2);
        assertThat(table.columnWithName("c3")).isSameAs(c3);
        assertThat(table.columnWithName("c4")).isSameAs(c4);
    }

    @Test
    public void shouldNotFindNonExistantColumnsByName() {
        assertThat(table.columnWithName("c1 ")).isNull();
        assertThat(table.columnWithName("wrong")).isNull();
    }

    @Test
    public void shouldHavePrimaryKeyColumns() {
        assertThat(table.primaryKeyColumnNames()).containsExactly("C1", "C2");
        assertThat(table.primaryKeyColumns()).containsExactly(c1, c2);
    }

    @Test
    public void shouldDetermineIfColumnIsPartOfPrimaryKeyUsingColumnNameWithExactMatch() {
        assertThat(table.isPrimaryKeyColumn("C1")).isTrue();
        assertThat(table.isPrimaryKeyColumn("C2")).isTrue();
        assertThat(table.isPrimaryKeyColumn("C3")).isFalse();
        assertThat(table.isPrimaryKeyColumn("C4")).isFalse();
        assertThat(table.isPrimaryKeyColumn("non-existant")).isFalse();
    }

    @Test
    public void shouldDetermineIfColumnIsPartOfPrimaryKeyUsingColumnNameWithWrongCase() {
        assertThat(table.isPrimaryKeyColumn("c1")).isTrue();
        assertThat(table.isPrimaryKeyColumn("c2")).isTrue();
        assertThat(table.isPrimaryKeyColumn("c3")).isFalse();
        assertThat(table.isPrimaryKeyColumn("c4")).isFalse();
        assertThat(table.isPrimaryKeyColumn("non-existant")).isFalse();
    }

    @Test
    public void shouldDetermineIfColumnIsGeneratedUsingColumnName() {
        assertThat(table.isGenerated("C1")).isTrue();
        assertThat(table.isGenerated("C2")).isFalse();
        assertThat(table.isGenerated("C3")).isFalse();
        assertThat(table.isGenerated("C4")).isFalse();
        assertThat(table.isGenerated("non-existant")).isFalse();
    }

    @Test
    public void shouldDetermineIfColumnIsAutoIncrementedUsingColumnName() {
        assertThat(table.isAutoIncremented("C1")).isFalse();
        assertThat(table.isAutoIncremented("C2")).isFalse();
        assertThat(table.isAutoIncremented("C3")).isFalse();
        assertThat(table.isAutoIncremented("C4")).isTrue();
        assertThat(table.isAutoIncremented("non-existant")).isFalse();
    }

    @Test
    public void shouldDetermineIfColumnIsOptionalUsingColumnName() {
        assertThat(table.isOptional("C1")).isFalse();
        assertThat(table.isOptional("C2")).isFalse();
        assertThat(table.isOptional("C3")).isTrue();
        assertThat(table.isOptional("C4")).isTrue();
        assertThat(table.isOptional("non-existant")).isFalse();
    }

    @Test
    public void shouldFilterColumnsUsingPredicate() {
        assertThat(table.filterColumns(c -> c.isAutoIncremented())).containsExactly(c4);
        assertThat(table.filterColumns(c -> c.isGenerated())).containsExactly(c1);
        assertThat(table.filterColumns(c -> c.isOptional())).containsExactly(c3, c4);
    }

    @Test
    public void shouldHaveToStringMethod() {
        String msg = table.toString();
        assertThat(msg).isNotNull();
        assertThat(msg).isNotEmpty();
    }

}
