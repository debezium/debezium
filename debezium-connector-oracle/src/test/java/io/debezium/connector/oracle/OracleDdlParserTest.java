/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Types;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.Test;

import io.debezium.ddl.parser.oracle.generated.PlSqlLexer;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Unit_statementContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

public class OracleDdlParserTest {

    @Test
    public void shouldParseCreateTable() {
        String ddl = "create table debezium.customer (" +
                "  id int not null, " +
                "  name varchar2(1000), " +
                "  score decimal(6, 2), " +
                "  registered date, " +
                "  primary key (id)" +
                ");";

        OracleDdlParser parser = new OracleDdlParser();
        parser.setCurrentDatabase("ORCLPDB1");
        parser.setCurrentSchema("DEBEZIUM");

        Tables tables = new Tables();
        parser.parse(ddl, tables);
        Table table = tables.forTable(new TableId("ORCLPDB1", "DEBEZIUM", "CUSTOMER"));

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

    private void printAst(String ddl) {
        PlSqlLexer lexer = new PlSqlLexer(new ANTLRInputStream(ddl));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PlSqlParser parser = new PlSqlParser(tokens);

        Unit_statementContext ast = parser.unit_statement();
        new AstPrinter().print(ast);
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
}
