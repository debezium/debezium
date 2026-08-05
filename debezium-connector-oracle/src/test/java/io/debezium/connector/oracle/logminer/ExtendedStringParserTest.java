/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.parser.ExtendedStringParser;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * @author Sergei Nikolaev
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER)
public class ExtendedStringParserTest {

    private static final ExtendedStringParser parser = new ExtendedStringParser();

    @Test
    @FixFor("debezium/dbz#2366")
    public void shouldParseExtendedStringBeginRedoSqlWithTableAlias() {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST_TABLE"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("DATA").create())
                .create();

        final String redoSql = "DECLARE\n" +
                " TempLob CLOB;\n" +
                " buf_c   VARCHAR2(32767);\n" +
                " Stmt    CLOB;\n" +
                "BEGIN\n" +
                " DBMS_LOB.CreateTemporary(TempLob, FALSE);\n" +
                " Stmt := 'update \"DEBEZIUM\".\"TEST_TABLE\" a set a.\"DATA\" = :DATA where a.\"ID\" = ''1'';';";

        final LogMinerDmlEntry entry = parser.parse(redoSql, table);

        assertThat(parser.getColumnName()).isEqualTo("DATA");
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
        assertThat(entry.getOldValues()[0]).isEqualTo("1");
    }

    @Test
    @FixFor("debezium/dbz#2366")
    public void shouldParseExtendedStringBeginRedoSqlWithoutTableAlias() {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST_TABLE"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("DATA").create())
                .create();

        final String redoSql = "DECLARE\n" +
                " TempLob CLOB;\n" +
                " buf_c   VARCHAR2(32767);\n" +
                " Stmt    CLOB;\n" +
                "BEGIN\n" +
                " DBMS_LOB.CreateTemporary(TempLob, FALSE);\n" +
                " Stmt := 'update \"DEBEZIUM\".\"TEST_TABLE\" set \"DATA\" = :DATA where \"ID\" = ''1'';';";

        final LogMinerDmlEntry entry = parser.parse(redoSql, table);

        assertThat(parser.getColumnName()).isEqualTo("DATA");
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
        assertThat(entry.getOldValues()[0]).isEqualTo("1");
    }
}
