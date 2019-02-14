/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParserListener;
import io.debezium.util.IoUtil;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.Before;
import org.junit.Test;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Types;
import java.util.Optional;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;


/**
 * This is the test suite for Oracle Antlr parser unit testing
 */
public class OracleDdlParserTest {

    private OracleDdlParser parser;
    private Tables tables;

    @Before
    public void setUp(){
        parser = new OracleDdlParser(true, null, null);
        tables = new Tables();
    }

    @Test
    public void shouldParseCreateAndAlterTable() {
        String createStatement = readFile("ddl/create_table.sql");
        assert createStatement != null;
        parser.parse(createStatement, tables);
        Table table = tables.forTable(new TableId(null, null, TABLE_NAME));
        //System.out.println(tables);

        assertThat(tables.size()).isEqualTo(1);
        assertThat(table.retrieveColumnNames()).containsExactly("id","col1","col2","col3","col4","col5","col6","col7","col8","col9","col10");
        // ID, primary key
        assertThat(table.columnWithName("id").position()).isEqualTo(1);
        assertThat(table.isPrimaryKeyColumn("ID"));
        testColumn(table, "id", false, Types.NUMERIC, "NUMBER", 19, 0,false, null);
        // number(4,2)
        testColumn(table, "col1", true, Types.NUMERIC, "NUMBER", 4, 2, true,null);
        // varchar2(255) default 'debezium'
        testColumn(table, "col2", false, Types.VARCHAR, "VARCHAR2", 255, null, true, "debezium");
        // nvarchar2(255)
        testColumn(table, "col3", false, Types.NVARCHAR, "NVARCHAR2", 255, null, false, null);
        // char(4)
        testColumn(table, "col4", true, Types.CHAR, "CHAR", 1, null, true, null);
        // nchar(4)
        testColumn(table, "col5", true, Types.NCHAR, "NCHAR", 1, 0, true, null);
        // float(126)
        testColumn(table, "col6", true, Types.FLOAT, "FLOAT", 126, 0, true, null);
        // date
        testColumn(table, "col7", true, Types.TIMESTAMP, "DATE", -1,  null,true, null);
        // timestamp
        testColumn(table, "col8", true, Types.TIMESTAMP, "TIMESTAMP", 6, null, true, null);
        // blob
        testColumn(table, "col9", true, Types.BLOB, "BLOB", -1,  null,true, null);
        // clob
        testColumn(table, "col10", true, Types.CLOB, "CLOB", -1,  null,true, null);
        // todo sdo_geometry
        //testColumn(table, "col12", true, Types.STRUCT, "MDSYS.SDO_GEOMETRY", -1,  null,true);

        String ddl = "alter table " + TABLE_NAME + " add (col21 varchar2(20), col22 number(19));";
        parser.parse(ddl, tables);
        Table alteredTable = tables.forTable(new TableId(null, null, TABLE_NAME));
        assertThat(alteredTable.retrieveColumnNames()).containsExactly("id","col1","col2","col3","col4","col5","col6","col7","col8","col9","col10", "col21", "col22");
        // varchar2(255)
        testColumn(alteredTable, "col21", true, Types.VARCHAR, "VARCHAR2", 20, null, true, null);
        testColumn(alteredTable, "col22", true, Types.NUMERIC, "NUMBER", 19, 0, true, null);

        //System.out.println(tables);

        ddl = "alter table " + TABLE_NAME + " add col23 varchar2(20);";
        try {
            parser.parse(ddl, tables);
        } catch (Exception e) {
            // Although parenthesis are optional for Oracle, but must be presented to be parsed, Antlr source error?
            assertThat(e.getMessage().contains("no viable alternative at input"));
        }
        ddl = "alter table " + TABLE_NAME + " add (col23 varchar2(20) not null);";
        parser.parse(ddl, tables);
        alteredTable = tables.forTable(new TableId(null, null, TABLE_NAME));
        assertThat(alteredTable.retrieveColumnNames()).containsExactly("id","col1","col2","col3","col4","col5","col6","col7","col8","col9","col10", "col21", "col22", "col23");
        testColumn(alteredTable, "col23", false, Types.VARCHAR, "VARCHAR2", 20, null, false, null);

        ddl = "alter table " + TABLE_NAME + " drop (col22, col23);";
        parser.parse(ddl, tables);
        alteredTable = tables.forTable(new TableId(null, null, TABLE_NAME));
        assertThat(alteredTable.retrieveColumnNames()).containsExactly("id","col1","col2","col3","col4","col5","col6","col7","col8","col9","col10", "col21");

        //System.out.println(tables);

        ddl = "drop table " + TABLE_NAME +";";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isZero();

        // todo
        ddl = "drop table " + TABLE_NAME + " cascade constrains purge;";
        ddl = "ALTER TABLE "+TABLE_NAME+" ADD CONSTRAINT FKB97209E040C4205 FOREIGN KEY (col1) REFERENCES debezium_ref(ID);";
        ddl = "ALTER TABLE "+TABLE_NAME+" MODIFY COL1 varchar2(50) not null;";
    }

    /**
     * this is original test by Gunnar Morling
     */
    @Test
    public void shouldParseCreateTable() {

        parser.setCurrentDatabase("ORCLPDB1");
        parser.setCurrentSchema(TABLE_NAME);

        String CREATE_SIMPLE_TABLE = "create table debezium.customer (" +
                "  id int not null, " +
                "  name varchar2(1000), " +
                "  score decimal(6, 2), " +
                "  registered date, " +
                "  primary key (id)" +
                ");";
        parser.parse(CREATE_SIMPLE_TABLE, tables);
        Table table = tables.forTable(new TableId("ORCLPDB1", "DEBEZIUM", "customer"));

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
        Optional oScale = column.scale();
        if (oScale.isPresent()){
            assertThat(oScale.get()).isEqualTo(scale);
        }
        assertThat(column.hasDefaultValue()).isEqualTo(hasDefault);
        if (column.hasDefaultValue() && column.defaultValue() != null) {
            assertThat(defaultValue.equals(column.defaultValue()));
        }
    }

    protected void printEvent(DdlParserListener.Event event) {
        System.out.println(event);
    }

    private static class AstPrinter {

        public void print(RuleContext ctx) {
            explore(ctx, 0);
        }

        private void explore(RuleContext ctx, int indentation) {
            String ruleName = PlSqlParser.ruleNames[ctx.getRuleIndex()];
            for (int i=0;i<indentation;i++) {
                System.out.print("  ");
            }
            System.out.println(ruleName);
            for (int i=0;i<ctx.getChildCount();i++) {
                ParseTree element = ctx.getChild(i);
                if (element instanceof RuleContext) {
                    explore((RuleContext)element, indentation + 1);
                }
            }
        }
    }

    private String readFile(String classpathResource) {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream(classpathResource);) {
            assertThat(stream).isNotNull();
            return IoUtil.read(stream);
        } catch (IOException e) {
            fail("Unable to read '" + classpathResource + "'");
        }
        assert false : "should never get here";
        return null;
    }
    private static final String TABLE_NAME = "DEBEZIUM";
}
