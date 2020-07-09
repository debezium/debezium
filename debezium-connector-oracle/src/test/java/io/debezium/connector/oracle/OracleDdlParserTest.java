/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Types;
import java.util.Objects;
import java.util.Optional;

import javax.validation.constraints.NotNull;

import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.IoUtil;

/**
 * This is the test suite for Oracle Antlr parser unit testing
 */
public class OracleDdlParserTest {

    private static final String TABLE_NAME = "TEST";
    private static final String PDB_NAME = "ORCLPDB1";

    private OracleDdlParser parser;
    private Tables tables;

    @Before
    public void setUp() {
        parser = new OracleDdlParser(true, null, null);
        tables = new Tables();
    }

    @Test
    public void shouldParseCreateAndAlterTable() throws Exception {
        final String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        Objects.requireNonNull(createStatement);
        parser.parse(createStatement, tables);
        Table table = tables.forTable(new TableId(null, null, TABLE_NAME));

        assertThat(tables.size()).isEqualTo(1);
        assertThat(table.retrieveColumnNames()).containsExactly("ID", "COL1", "COL2", "COL3", "COL4", "COL5", "COL6", "COL8", "COL9", "COL10", "COL11", "COL12");
        // ID, primary key
        assertThat(table.columnWithName("ID").position()).isEqualTo(1);
        assertThat(table.isPrimaryKeyColumn("ID"));
        testColumn(table, "ID", false, Types.NUMERIC, "NUMBER", 19, 0, false, null);
        // number(4,2)
        testColumn(table, "COL1", true, Types.NUMERIC, "NUMBER", 4, 2, true, null);
        // varchar2(255) default 'debezium'
        testColumn(table, "COL2", false, Types.VARCHAR, "VARCHAR2", 255, null, true, "debezium");
        // nvarchar2(255)
        testColumn(table, "COL3", false, Types.NVARCHAR, "NVARCHAR2", 255, null, false, null);
        // char(4)
        testColumn(table, "COL4", true, Types.CHAR, "CHAR", 1, null, true, null);
        // nchar(4)
        testColumn(table, "COL5", true, Types.NCHAR, "NCHAR", 1, 0, true, null);
        // float(126)
        testColumn(table, "COL6", true, Types.FLOAT, "FLOAT", 126, 0, true, null);
        // todo: DBZ-137 removed
        // date
        // testColumn(table, "COL7", true, Types.TIMESTAMP, "DATE", -1, null, true, null);
        // timestamp
        testColumn(table, "COL8", true, Types.TIMESTAMP, "TIMESTAMP", 6, null, true, null);
        // blob
        testColumn(table, "COL9", true, Types.BLOB, "BLOB", -1, null, true, null);
        // clob
        testColumn(table, "COL10", true, Types.CLOB, "CLOB", -1, null, true, null);
        // sdo_geometry
        testColumn(table, "col11", true, Types.STRUCT, "MDSYS.SDO_GEOMETRY", -1, null, true, null);
        // number(1,0)
        testColumn(table, "col12", true, Types.NUMERIC, "NUMBER", 1, 0, true, null);

        String ddl = "alter table " + TABLE_NAME + " add (col21 varchar2(20), col22 number(19));";
        parser.parse(ddl, tables);
        Table alteredTable = tables.forTable(new TableId(null, null, TABLE_NAME));
        assertThat(alteredTable.retrieveColumnNames()).containsExactly("ID", "COL1", "COL2", "COL3", "COL4", "COL5", "COL6", "COL8", "COL9", "COL10", "COL11", "COL12",
                "COL21",
                "COL22");
        // varchar2(255)
        testColumn(alteredTable, "COL21", true, Types.VARCHAR, "VARCHAR2", 20, null, true, null);
        testColumn(alteredTable, "COL22", true, Types.NUMERIC, "NUMBER", 19, 0, true, null);

        // todo check real LogMiner entry, maybe this entry never happens
        ddl = "alter table " + TABLE_NAME + " add col23 varchar2(20);";
        try {
            parser.parse(ddl, tables);
        }
        catch (Exception e) {
            // Although parenthesis are optional for Oracle, but must be presented to be parsed, Antlr source error?
            assertThat(e.getMessage().contains("no viable alternative at input"));
        }
        ddl = "alter table " + TABLE_NAME + " add (col23 varchar2(20) not null);";
        parser.parse(ddl, tables);
        alteredTable = tables.forTable(new TableId(null, null, TABLE_NAME));
        assertThat(alteredTable.retrieveColumnNames()).containsExactly("ID", "COL1", "COL2", "COL3", "COL4", "COL5", "COL6", "COL8", "COL9", "COL10", "COL11", "COL12",
                "COL21",
                "COL22", "COL23");
        testColumn(alteredTable, "COL23", false, Types.VARCHAR, "VARCHAR2", 20, null, false, null);

        ddl = "alter table " + TABLE_NAME + " drop (col22, col23);";
        parser.parse(ddl, tables);
        alteredTable = tables.forTable(new TableId(null, null, TABLE_NAME));
        assertThat(alteredTable.retrieveColumnNames()).containsExactly("ID", "COL1", "COL2", "COL3", "COL4", "COL5", "COL6", "COL8", "COL9", "COL10", "COL11", "COL12",
                "COL21");

        ddl = "drop table " + TABLE_NAME + ";";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isZero();

        // todo
        ddl = "drop table " + TABLE_NAME + " cascade constrains purge;";
        ddl = "ALTER TABLE " + TABLE_NAME + " ADD CONSTRAINT FKB97209E040C4205 FOREIGN KEY (col1) REFERENCES debezium_ref(ID);";
        ddl = "ALTER TABLE " + TABLE_NAME + " MODIFY COL1 varchar2(50) not null;";
    }

    /**
     * this is original test by Gunnar Morling
     */
    @Test
    public void shouldParseCreateTable() {

        parser.setCurrentDatabase(PDB_NAME);
        parser.setCurrentSchema("DEBEZIUM");

        String CREATE_SIMPLE_TABLE = "create table debezium.customer (" +
                "  id int not null, " +
                "  name varchar2(1000), " +
                "  score decimal(6, 2), " +
                "  registered date, " +
                "  primary key (id)" +
                ");";
        parser.parse(CREATE_SIMPLE_TABLE, tables);
        Table table = tables.forTable(new TableId(PDB_NAME, "DEBEZIUM", "CUSTOMER"));

        assertThat(table).isNotNull();

        Column id = table.columnWithName("ID");
        assertThat(id.isOptional()).isFalse();
        assertThat(id.jdbcType()).isEqualTo(Types.NUMERIC);
        assertThat(id.typeName()).isEqualTo("NUMBER");

        final Column name = table.columnWithName("NAME");
        assertThat(name.isOptional()).isTrue();
        assertThat(name.jdbcType()).isEqualTo(Types.VARCHAR);
        assertThat(name.typeName()).isEqualTo("VARCHAR2");
        assertThat(name.length()).isEqualTo(1000);

        final Column score = table.columnWithName("SCORE");
        assertThat(score.isOptional()).isTrue();
        assertThat(score.jdbcType()).isEqualTo(Types.NUMERIC);
        assertThat(score.typeName()).isEqualTo("NUMBER");
        assertThat(score.length()).isEqualTo(6);
        assertThat(score.scale().get()).isEqualTo(2);

        assertThat(table.columns()).hasSize(4);
        assertThat(table.isPrimaryKeyColumn("ID"));
    }

    private void testColumn(@NotNull Table table, @NotNull String name, boolean isOptional,
                            Integer jdbcType, String typeName, Integer length, Integer scale,
                            Boolean hasDefault, Object defaultValue) {
        Column column = table.columnWithName(name);
        assertThat(column.isOptional()).isEqualTo(isOptional);
        assertThat(column.jdbcType()).isEqualTo(jdbcType);
        assertThat(column.typeName()).isEqualTo(typeName);
        assertThat(column.length()).isEqualTo(length);
        Optional<Integer> oScale = column.scale();
        if (oScale.isPresent()) {
            assertThat(oScale.get()).isEqualTo(scale);
        }
        assertThat(column.hasDefaultValue()).isEqualTo(hasDefault);
        if (column.hasDefaultValue() && column.defaultValue() != null) {
            assertThat(defaultValue.equals(column.defaultValue()));
        }
    }
}
