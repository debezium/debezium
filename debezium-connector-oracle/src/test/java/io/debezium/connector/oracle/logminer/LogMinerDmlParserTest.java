/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.oracle.logminer.parser.LogMinerDmlParser;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.data.Envelope.Operation;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * @author Chris Cranford
 */
public class LogMinerDmlParserTest {

    private LogMinerDmlParser fastDmlParser;

    @Before
    public void beforeEach() throws Exception {
        // Create LogMinerDmlParser
        fastDmlParser = new LogMinerDmlParser();
    }

    // Oracle's generated SQL avoids common spacing patterns such as spaces between column values or values
    // in an insert statement and is explicit about spacing and commas with SET and WHERE clauses. As of
    // now the parser expects this explicit spacing usage.

    @Test
    @FixFor("DBZ-3078")
    public void testParsingInsert() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("NAME").create())
                .addColumn(Column.editor().name("TS").create())
                .addColumn(Column.editor().name("UT").create())
                .addColumn(Column.editor().name("DATE").create())
                .addColumn(Column.editor().name("UT2").create())
                .addColumn(Column.editor().name("C1").create())
                .addColumn(Column.editor().name("C2").create())
                .addColumn(Column.editor().name("UNUSED").create())
                .create();

        String sql = "insert into \"DEBEZIUM\".\"TEST\"(\"ID\",\"NAME\",\"TS\",\"UT\",\"DATE\",\"UT2\",\"C1\",\"C2\") values " +
                "('1','Acme',TO_TIMESTAMP('2020-02-01 00:00:00.'),Unsupported Type," +
                "TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),Unsupported Type,NULL,NULL);";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getCommandType()).isEqualTo(Operation.CREATE);
        assertThat(entry.getOldValues()).isEmpty();
        assertThat(entry.getNewValues()).hasSize(9);
        assertThat(entry.getNewValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getNewValues().get(1).getColumnName()).isEqualTo("NAME");
        assertThat(entry.getNewValues().get(2).getColumnName()).isEqualTo("TS");
        assertThat(entry.getNewValues().get(3).getColumnName()).isEqualTo("UT");
        assertThat(entry.getNewValues().get(4).getColumnName()).isEqualTo("DATE");
        assertThat(entry.getNewValues().get(5).getColumnName()).isEqualTo("UT2");
        assertThat(entry.getNewValues().get(6).getColumnName()).isEqualTo("C1");
        assertThat(entry.getNewValues().get(7).getColumnName()).isEqualTo("C2");
        assertThat(entry.getNewValues().get(8).getColumnName()).isEqualTo("UNUSED");
        assertThat(entry.getNewValues().get(0).getColumnData()).isEqualTo("1");
        assertThat(entry.getNewValues().get(1).getColumnData()).isEqualTo("Acme");
        assertThat(entry.getNewValues().get(2).getColumnData()).isEqualTo("TO_TIMESTAMP('2020-02-01 00:00:00.')");
        assertThat(entry.getNewValues().get(3).getColumnData()).isNull();
        assertThat(entry.getNewValues().get(4).getColumnData()).isEqualTo("TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')");
        assertThat(entry.getNewValues().get(5).getColumnData()).isNull();
        assertThat(entry.getNewValues().get(6).getColumnData()).isNull();
        assertThat(entry.getNewValues().get(7).getColumnData()).isNull();
        assertThat(entry.getNewValues().get(8).getColumnData()).isNull();
    }

    @Test
    @FixFor("DBZ-3078")
    public void testParsingUpdate() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("NAME").create())
                .addColumn(Column.editor().name("TS").create())
                .addColumn(Column.editor().name("UT").create())
                .addColumn(Column.editor().name("DATE").create())
                .addColumn(Column.editor().name("UT2").create())
                .addColumn(Column.editor().name("C1").create())
                .addColumn(Column.editor().name("IS").create())
                .addColumn(Column.editor().name("IS2").create())
                .addColumn(Column.editor().name("UNUSED").create())
                .create();

        String sql = "update \"DEBEZIUM\".\"TEST\" " +
                "set \"NAME\" = 'Bob', \"TS\" = TO_TIMESTAMP('2020-02-02 00:00:00.'), \"UT\" = Unsupported Type, " +
                "\"DATE\" = TO_DATE('2020-02-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), \"UT2\" = Unsupported Type, " +
                "\"C1\" = NULL where \"ID\" = '1' and \"NAME\" = 'Acme' and \"TS\" = TO_TIMESTAMP('2020-02-01 00:00:00.') and " +
                "\"UT\" = Unsupported Type and \"DATE\" = TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') and " +
                "\"UT2\" = Unsupported Type and \"C1\" = NULL and \"IS\" IS NULL and \"IS2\" IS NULL;";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getCommandType()).isEqualTo(Operation.UPDATE);
        assertThat(entry.getOldValues()).hasSize(10);
        assertThat(entry.getOldValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getOldValues().get(1).getColumnName()).isEqualTo("NAME");
        assertThat(entry.getOldValues().get(2).getColumnName()).isEqualTo("TS");
        assertThat(entry.getOldValues().get(3).getColumnName()).isEqualTo("UT");
        assertThat(entry.getOldValues().get(4).getColumnName()).isEqualTo("DATE");
        assertThat(entry.getOldValues().get(5).getColumnName()).isEqualTo("UT2");
        assertThat(entry.getOldValues().get(6).getColumnName()).isEqualTo("C1");
        assertThat(entry.getOldValues().get(7).getColumnName()).isEqualTo("IS");
        assertThat(entry.getOldValues().get(8).getColumnName()).isEqualTo("IS2");
        assertThat(entry.getOldValues().get(9).getColumnName()).isEqualTo("UNUSED");
        assertThat(entry.getOldValues().get(0).getColumnData()).isEqualTo("1");
        assertThat(entry.getOldValues().get(1).getColumnData()).isEqualTo("Acme");
        assertThat(entry.getOldValues().get(2).getColumnData()).isEqualTo("TO_TIMESTAMP('2020-02-01 00:00:00.')");
        assertThat(entry.getOldValues().get(3).getColumnData()).isNull();
        assertThat(entry.getOldValues().get(4).getColumnData()).isEqualTo("TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')");
        assertThat(entry.getOldValues().get(5).getColumnData()).isNull();
        assertThat(entry.getOldValues().get(6).getColumnData()).isNull();
        assertThat(entry.getOldValues().get(7).getColumnData()).isNull();
        assertThat(entry.getOldValues().get(8).getColumnData()).isNull();
        assertThat(entry.getOldValues().get(9).getColumnData()).isNull();
        assertThat(entry.getNewValues()).hasSize(10);
        assertThat(entry.getNewValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getNewValues().get(1).getColumnName()).isEqualTo("NAME");
        assertThat(entry.getNewValues().get(2).getColumnName()).isEqualTo("TS");
        assertThat(entry.getNewValues().get(3).getColumnName()).isEqualTo("UT");
        assertThat(entry.getNewValues().get(4).getColumnName()).isEqualTo("DATE");
        assertThat(entry.getNewValues().get(5).getColumnName()).isEqualTo("UT2");
        assertThat(entry.getNewValues().get(6).getColumnName()).isEqualTo("C1");
        assertThat(entry.getNewValues().get(7).getColumnName()).isEqualTo("IS");
        assertThat(entry.getNewValues().get(8).getColumnName()).isEqualTo("IS2");
        assertThat(entry.getNewValues().get(9).getColumnName()).isEqualTo("UNUSED");
        assertThat(entry.getNewValues().get(0).getColumnData()).isEqualTo("1");
        assertThat(entry.getNewValues().get(1).getColumnData()).isEqualTo("Bob");
        assertThat(entry.getNewValues().get(2).getColumnData()).isEqualTo("TO_TIMESTAMP('2020-02-02 00:00:00.')");
        assertThat(entry.getNewValues().get(3).getColumnData()).isNull();
        assertThat(entry.getNewValues().get(4).getColumnData()).isEqualTo("TO_DATE('2020-02-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')");
        assertThat(entry.getNewValues().get(5).getColumnData()).isNull();
        assertThat(entry.getNewValues().get(6).getColumnData()).isNull();
        assertThat(entry.getNewValues().get(7).getColumnData()).isNull();
        assertThat(entry.getNewValues().get(8).getColumnData()).isNull();
        assertThat(entry.getNewValues().get(9).getColumnData()).isNull();
    }

    @Test
    @FixFor("DBZ-3078")
    public void testParsingDelete() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("NAME").create())
                .addColumn(Column.editor().name("TS").create())
                .addColumn(Column.editor().name("UT").create())
                .addColumn(Column.editor().name("DATE").create())
                .addColumn(Column.editor().name("IS").create())
                .addColumn(Column.editor().name("IS2").create())
                .addColumn(Column.editor().name("UNUSED").create())
                .create();

        String sql = "delete from \"DEBEZIUM\".\"TEST\" " +
                "where \"ID\" = '1' and \"NAME\" = 'Acme' and \"TS\" = TO_TIMESTAMP('2020-02-01 00:00:00.') and " +
                "\"UT\" = Unsupported Type and \"DATE\" = TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') and " +
                "\"IS\" IS NULL and \"IS2\" IS NULL;";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getCommandType()).isEqualTo(Operation.DELETE);
        assertThat(entry.getOldValues()).hasSize(8);
        assertThat(entry.getOldValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getOldValues().get(1).getColumnName()).isEqualTo("NAME");
        assertThat(entry.getOldValues().get(2).getColumnName()).isEqualTo("TS");
        assertThat(entry.getOldValues().get(3).getColumnName()).isEqualTo("UT");
        assertThat(entry.getOldValues().get(4).getColumnName()).isEqualTo("DATE");
        assertThat(entry.getOldValues().get(5).getColumnName()).isEqualTo("IS");
        assertThat(entry.getOldValues().get(6).getColumnName()).isEqualTo("IS2");
        assertThat(entry.getOldValues().get(7).getColumnName()).isEqualTo("UNUSED");
        assertThat(entry.getOldValues().get(0).getColumnData()).isEqualTo("1");
        assertThat(entry.getOldValues().get(1).getColumnData()).isEqualTo("Acme");
        assertThat(entry.getOldValues().get(2).getColumnData()).isEqualTo("TO_TIMESTAMP('2020-02-01 00:00:00.')");
        assertThat(entry.getOldValues().get(3).getColumnData()).isNull();
        assertThat(entry.getOldValues().get(4).getColumnData()).isEqualTo("TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')");
        assertThat(entry.getOldValues().get(5).getColumnData()).isNull();
        assertThat(entry.getOldValues().get(6).getColumnData()).isNull();
        assertThat(entry.getOldValues().get(7).getColumnData()).isNull();
        assertThat(entry.getNewValues()).isEmpty();
    }

    @Test
    @FixFor("DBZ-3235")
    public void testParsingUpdateWithNoWhereClauseIsAcceptable() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST"))
                .addColumn(Column.editor().name("COL1").create())
                .addColumn(Column.editor().name("COL2").create())
                .addColumn(Column.editor().name("COL3").create())
                .addColumn(Column.editor().name("UNUSED").create())
                .create();

        String sql = "update \"DEBEZIUM\".\"TEST\" set \"COL1\" = '1', \"COL2\" = NULL, \"COL3\" = 'Hello';";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getCommandType()).isEqualTo(Operation.UPDATE);
        assertThat(entry.getOldValues()).isEmpty();
        assertThat(entry.getNewValues()).hasSize(4);
        assertThat(entry.getNewValues().get(0).getColumnName()).isEqualTo("COL1");
        assertThat(entry.getNewValues().get(0).getColumnData()).isEqualTo("1");
        assertThat(entry.getNewValues().get(1).getColumnName()).isEqualTo("COL2");
        assertThat(entry.getNewValues().get(1).getColumnData()).isNull();
        assertThat(entry.getNewValues().get(2).getColumnName()).isEqualTo("COL3");
        assertThat(entry.getNewValues().get(2).getColumnData()).isEqualTo("Hello");
        assertThat(entry.getNewValues().get(3).getColumnName()).isEqualTo("UNUSED");
        assertThat(entry.getNewValues().get(3).getColumnData()).isNull();
    }

    @Test
    @FixFor("DBZ-3235")
    public void testParsingDeleteWithNoWhereClauseIsAcceptable() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST"))
                .addColumn(Column.editor().name("COL1").create())
                .addColumn(Column.editor().name("COL2").create())
                .addColumn(Column.editor().name("COL3").create())
                .addColumn(Column.editor().name("UNUSED").create())
                .create();

        String sql = "delete from \"DEBEZIUM\".\"TEST\";";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getCommandType()).isEqualTo(Operation.DELETE);
        assertThat(entry.getOldValues()).isEmpty();
        assertThat(entry.getNewValues()).isEmpty();
    }
}
