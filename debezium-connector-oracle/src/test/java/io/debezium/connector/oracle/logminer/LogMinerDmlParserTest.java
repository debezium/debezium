/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.parser.LogMinerColumnResolverDmlParser;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlParser;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import oracle.jdbc.OracleTypes;

/**
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER)
public class LogMinerDmlParserTest {

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    private LogMinerDmlParser fastDmlParser;
    private LogMinerColumnResolverDmlParser columnResolverDmlParser;

    @Before
    public void beforeEach() throws Exception {
        fastDmlParser = new LogMinerDmlParser();
        columnResolverDmlParser = new LogMinerColumnResolverDmlParser();
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

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.INSERT);
        assertThat(entry.getOldValues()).isEmpty();
        assertThat(entry.getNewValues()).hasSize(9);
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
        assertThat(entry.getNewValues()[1]).isEqualTo("Acme");
        assertThat(entry.getNewValues()[2]).isEqualTo("TO_TIMESTAMP('2020-02-01 00:00:00.')");
        assertThat(entry.getNewValues()[3]).isNull();
        assertThat(entry.getNewValues()[4]).isEqualTo("TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')");
        assertThat(entry.getNewValues()[5]).isNull();
        assertThat(entry.getNewValues()[6]).isNull();
        assertThat(entry.getNewValues()[7]).isNull();
        assertThat(entry.getNewValues()[8]).isNull();
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

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.UPDATE);
        assertThat(entry.getOldValues()).hasSize(10);
        assertThat(entry.getOldValues()[0]).isEqualTo("1");
        assertThat(entry.getOldValues()[1]).isEqualTo("Acme");
        assertThat(entry.getOldValues()[2]).isEqualTo("TO_TIMESTAMP('2020-02-01 00:00:00.')");
        assertThat(entry.getOldValues()[3]).isNull();
        assertThat(entry.getOldValues()[4]).isEqualTo("TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')");
        assertThat(entry.getOldValues()[5]).isNull();
        assertThat(entry.getOldValues()[6]).isNull();
        assertThat(entry.getOldValues()[7]).isNull();
        assertThat(entry.getOldValues()[8]).isNull();
        assertThat(entry.getOldValues()[9]).isNull();
        assertThat(entry.getNewValues()).hasSize(10);
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
        assertThat(entry.getNewValues()[1]).isEqualTo("Bob");
        assertThat(entry.getNewValues()[2]).isEqualTo("TO_TIMESTAMP('2020-02-02 00:00:00.')");
        assertThat(entry.getNewValues()[3]).isNull();
        assertThat(entry.getNewValues()[4]).isEqualTo("TO_DATE('2020-02-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')");
        assertThat(entry.getNewValues()[5]).isNull();
        assertThat(entry.getNewValues()[6]).isNull();
        assertThat(entry.getNewValues()[7]).isNull();
        assertThat(entry.getNewValues()[8]).isNull();
        assertThat(entry.getNewValues()[9]).isNull();
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

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.DELETE);
        assertThat(entry.getOldValues()).hasSize(8);
        assertThat(entry.getOldValues()[0]).isEqualTo("1");
        assertThat(entry.getOldValues()[1]).isEqualTo("Acme");
        assertThat(entry.getOldValues()[2]).isEqualTo("TO_TIMESTAMP('2020-02-01 00:00:00.')");
        assertThat(entry.getOldValues()[3]).isNull();
        assertThat(entry.getOldValues()[4]).isEqualTo("TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')");
        assertThat(entry.getOldValues()[5]).isNull();
        assertThat(entry.getOldValues()[6]).isNull();
        assertThat(entry.getOldValues()[7]).isNull();
        assertThat(entry.getNewValues()).isEmpty();
    }

    @Test
    @FixFor({ "DBZ-3235", "DBZ-4194" })
    public void testParsingUpdateWithNoWhereClauseIsAcceptable() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST"))
                .addColumn(Column.editor().name("COL1").create())
                .addColumn(Column.editor().name("COL2").create())
                .addColumn(Column.editor().name("COL3").create())
                .addColumn(Column.editor().name("UNUSED").create())
                .create();

        String sql = "update \"DEBEZIUM\".\"TEST\" set \"COL1\" = '1', \"COL2\" = NULL, \"COL3\" = 'Hello';";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.UPDATE);
        assertThat(entry.getOldValues()).hasSize(4);
        assertThat(entry.getOldValues()[0]).isNull();
        assertThat(entry.getOldValues()[1]).isNull();
        assertThat(entry.getOldValues()[2]).isNull();
        assertThat(entry.getOldValues()[3]).isNull();
        assertThat(entry.getNewValues()).hasSize(4);
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
        assertThat(entry.getNewValues()[1]).isNull();
        assertThat(entry.getNewValues()[2]).isEqualTo("Hello");
        assertThat(entry.getNewValues()[3]).isNull();
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

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.DELETE);
        assertThat(entry.getOldValues()).hasSize(4);
        assertThat(entry.getOldValues()[0]).isNull();
        assertThat(entry.getOldValues()[1]).isNull();
        assertThat(entry.getOldValues()[2]).isNull();
        assertThat(entry.getOldValues()[3]).isNull();
        assertThat(entry.getNewValues()).isEmpty();
    }

    @Test
    @FixFor("DBZ-4194")
    public void testParsingWithTableAliases() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST"))
                .addColumn(Column.editor().name("COL1").create())
                .addColumn(Column.editor().name("COL2").create())
                .addColumn(Column.editor().name("COL3").create())
                .addColumn(Column.editor().name("UNUSED").create())
                .create();

        String sql = "update \"DEBEZIUM\".\"TEST\" a set a.\"COL1\" = '1', a.\"COL2\" = NULL, a.\"COL3\" = 'Hello2';";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.UPDATE);
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
        assertThat(entry.getNewValues()[1]).isNull();
        assertThat(entry.getNewValues()[2]).isEqualTo("Hello2");

        sql = "delete from \"DEBEZIUM\".\"TEST\" a where a.\"COL1\" = '1' and a.\"COL2\" = '2' and a.\"COL3\" = Unsupported Type;";

        entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.DELETE);
        assertThat(entry.getOldValues()).hasSize(4);
        assertThat(entry.getOldValues()[0]).isEqualTo("1");
        assertThat(entry.getOldValues()[1]).isEqualTo("2");
        assertThat(entry.getOldValues()[2]).isNull();
        assertThat(entry.getOldValues()[3]).isNull();
        assertThat(entry.getNewValues()).isEmpty();
    }

    @Test
    @FixFor("DBZ-3258")
    public void testNameWithWhitespaces() throws Exception {
        final Table table = Table.editor()
                .tableId(new TableId(null, "UNKNOWN", "OBJ# 74858"))
                .addColumn(Column.editor().name("COL 1").create())
                .create();

        String sql = "insert into \"UNKNOWN\".\"OBJ# 74858\"(\"COL 1\") values (1)";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.INSERT);
        assertThat(entry.getOldValues()).isEmpty();
        assertThat(entry.getNewValues()).hasSize(1);
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
    }

    @Test
    @FixFor("DBZ-4891")
    public void testEscapedSingleQuote() throws Exception {
        final Table table = Table.editor()
                .tableId(new TableId(null, "UNKNOWN", "TABLE"))
                .addColumn(Column.editor().name("COL1").create())
                .create();

        String sql = "update \"UNKNOWN\".\"TABLE\" set \"COL1\" = 'I love ''Debezium''' where \"COL1\" = 'Use ''streams'' my friends'";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.UPDATE);
        assertThat(entry.getNewValues()).hasSize(1);
        assertThat(entry.getNewValues()[0]).isEqualTo("I love 'Debezium'");
        assertThat(entry.getOldValues()).hasSize(1);
        assertThat(entry.getOldValues()[0]).isEqualTo("Use 'streams' my friends");

        sql = "insert into \"UNKNOWN\".\"TABLE\" (\"COL1\") values ('''Debezium'' rulez')'";

        entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.INSERT);
        assertThat(entry.getNewValues()).hasSize(1);
        assertThat(entry.getNewValues()[0]).isEqualTo("'Debezium' rulez");
    }

    @Test
    @FixFor("DBZ-3305")
    public void testParsingUpdateWithNoWhereClauseFunctionAsLastColumn() throws Exception {
        final Table table = Table.editor()
                .tableId(new TableId(null, "TICKETUSER", "CRS_ORDER"))
                .addColumn(Column.editor().name("AMOUNT_PAID").create())
                .addColumn(Column.editor().name("AMOUNT_UNPAID").create())
                .addColumn(Column.editor().name("PAY_STATUS").create())
                .addColumn(Column.editor().name("IS_DEL").create())
                .addColumn(Column.editor().name("TM_UPDATE").create())
                .create();
        String sql = "update \"TICKETUSER\".\"CRS_ORDER\" set \"AMOUNT_PAID\" = '0', \"AMOUNT_UNPAID\" = '540', " +
                "\"PAY_STATUS\" = '10111015', \"IS_DEL\" = '0', \"TM_UPDATE\" = TO_DATE('2021-03-17 10:18:55', 'YYYY-MM-DD HH24:MI:SS');";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.UPDATE);
        assertThat(entry.getOldValues()).hasSize(5);
        assertThat(entry.getOldValues()[0]).isNull();
        assertThat(entry.getOldValues()[1]).isNull();
        assertThat(entry.getOldValues()[2]).isNull();
        assertThat(entry.getOldValues()[3]).isNull();
        assertThat(entry.getOldValues()[4]).isNull();
        assertThat(entry.getNewValues()).hasSize(5);
        assertThat(entry.getNewValues()[0]).isEqualTo("0");
        assertThat(entry.getNewValues()[1]).isEqualTo("540");
        assertThat(entry.getNewValues()[2]).isEqualTo("10111015");
        assertThat(entry.getNewValues()[3]).isEqualTo("0");
        assertThat(entry.getNewValues()[4]).isEqualTo("TO_DATE('2021-03-17 10:18:55', 'YYYY-MM-DD HH24:MI:SS')");
    }

    @Test
    @FixFor("DBZ-3367")
    public void shouldParsingRedoSqlWithParenthesisInFunctionArgumentStrings() throws Exception {
        final Table table = Table.editor()
                .tableId(new TableId(null, "DEBEZIUM", "TEST"))
                .addColumn(Column.editor().name("C1").create())
                .addColumn(Column.editor().name("C2").create())
                .create();

        String sql = "insert into \"DEBEZIUM\".\"TEST\" (\"C1\",\"C2\") values (UNISTR('\\963F\\72F8\\5C0F\\706B\\8F66\\5BB6\\5EAD\\7968(\\60CA\\559C\\FF09\\FF082161\\FF09'),NULL);";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.INSERT);
        assertThat(entry.getOldValues()).isEmpty();
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0])
                .isEqualTo("UNISTR('\\963F\\72F8\\5C0F\\706B\\8F66\\5BB6\\5EAD\\7968(\\60CA\\559C\\FF09\\FF082161\\FF09')");
        assertThat(entry.getNewValues()[1]).isNull();

        sql = "update \"DEBEZIUM\".\"TEST\" set " +
                "\"C2\" = UNISTR('\\963F\\72F8\\5C0F\\706B\\8F66\\5BB6\\5EAD\\7968(\\60CA\\559C\\FF09\\FF082161\\FF09') " +
                "where \"C1\" = UNISTR('\\963F\\72F8\\5C0F\\706B\\8F66\\5BB6\\5EAD\\7968(\\60CA\\559C\\FF09\\FF082161\\FF09');";
        entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.UPDATE);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0])
                .isEqualTo("UNISTR('\\963F\\72F8\\5C0F\\706B\\8F66\\5BB6\\5EAD\\7968(\\60CA\\559C\\FF09\\FF082161\\FF09')");
        assertThat(entry.getOldValues()[1]).isNull();
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0])
                .isEqualTo("UNISTR('\\963F\\72F8\\5C0F\\706B\\8F66\\5BB6\\5EAD\\7968(\\60CA\\559C\\FF09\\FF082161\\FF09')");
        assertThat(entry.getNewValues()[1])
                .isEqualTo("UNISTR('\\963F\\72F8\\5C0F\\706B\\8F66\\5BB6\\5EAD\\7968(\\60CA\\559C\\FF09\\FF082161\\FF09')");

        sql = "delete from \"DEBEZIUM\".\"TEST\" where \"C1\" = UNISTR('\\963F\\72F8\\5C0F\\706B\\8F66\\5BB6\\5EAD\\7968(\\60CA\\559C\\FF09\\FF082161\\FF09');";
        entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.DELETE);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0])
                .isEqualTo("UNISTR('\\963F\\72F8\\5C0F\\706B\\8F66\\5BB6\\5EAD\\7968(\\60CA\\559C\\FF09\\FF082161\\FF09')");
        assertThat(entry.getOldValues()[1]).isNull();
        assertThat(entry.getNewValues()).isEmpty();
    }

    @Test
    @FixFor("DBZ-3413")
    public void testParsingDoubleSingleQuoteInWhereClause() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST"))
                .addColumn(Column.editor().name("COL1").create())
                .addColumn(Column.editor().name("COL2").create())
                .create();

        String sql = "insert into \"DEBEZIUM\".\"TEST\"(\"COL1\",\"COL2\") values ('Bob''s dog','0');";
        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.INSERT);
        assertThat(entry.getOldValues()).isEmpty();
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0]).isEqualTo("Bob's dog");
        assertThat(entry.getNewValues()[1]).isEqualTo("0");

        sql = "update \"DEBEZIUM\".\"TEST\" set \"COL2\" = '1' where \"COL1\" = 'Bob''s dog' and \"COL2\" = '0';";
        entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.UPDATE);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0]).isEqualTo("Bob's dog");
        assertThat(entry.getOldValues()[1]).isEqualTo("0");
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0]).isEqualTo("Bob's dog");
        assertThat(entry.getNewValues()[1]).isEqualTo("1");

        sql = "delete from \"DEBEZIUM\".\"TEST\" where \"COL1\" = 'Bob''s dog' and \"COL2\" = '1';";
        entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.DELETE);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0]).isEqualTo("Bob's dog");
        assertThat(entry.getOldValues()[1]).isEqualTo("1");
        assertThat(entry.getNewValues()).isEmpty();
    }

    @Test
    @FixFor("DBZ-3892")
    public void shouldParseConcatenatedUnistrValues() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST"))
                .addColumn(Column.editor().name("COL1").create())
                .addColumn(Column.editor().name("COL2").create())
                .create();

        // test concatenation in INSERT column values
        String sql = "insert into \"DEBEZIUM\".\"TEST\"(\"COL1\",\"COL2\") values ('1',UNISTR('\0412\044B') || UNISTR('\043F\043E'));";
        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.INSERT);
        assertThat(entry.getOldValues()).isEmpty();
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
        assertThat(entry.getNewValues()[1]).isEqualTo("UNISTR('\0412\044B') || UNISTR('\043F\043E')");

        // test concatenation in SET statement
        sql = "update \"DEBEZIUM\".\"TEST\" set \"COL2\" = UNISTR('\0412\044B') || UNISTR('\043F\043E') where \"COL1\" = '1' and \"COL2\" IS NULL;";
        entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.UPDATE);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0]).isEqualTo("1");
        assertThat(entry.getOldValues()[1]).isNull();
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
        assertThat(entry.getNewValues()[1]).isEqualTo("UNISTR('\0412\044B') || UNISTR('\043F\043E')");

        // test concatenation in update WHERE statement
        sql = "update \"DEBEZIUM\".\"TEST\" set \"COL2\" = NULL where \"COL1\" = '1' and \"COL2\" = UNISTR('\0412\044B') || UNISTR('\043F\043E');";
        entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.UPDATE);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0]).isEqualTo("1");
        assertThat(entry.getOldValues()[1]).isEqualTo("UNISTR('\0412\044B') || UNISTR('\043F\043E')");
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
        assertThat(entry.getNewValues()[1]).isNull();

        // test concatenation in delete WHERE statement
        sql = "delete from \"DEBEZIUM\".\"TEST\" where \"COL1\" = '1' and \"COL2\" = UNISTR('\0412\044B') || UNISTR('\043F\043E');";
        entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.DELETE);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0]).isEqualTo("1");
        assertThat(entry.getOldValues()[1]).isEqualTo("UNISTR('\0412\044B') || UNISTR('\043F\043E')");
        assertThat(entry.getNewValues()).hasSize(0);
    }

    @Test
    public void shouldReturnUnavailableColumnValueForLobColumnTypes() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("COL1").create())
                .addColumn(Column.editor().name("VAL_CLOB").jdbcType(OracleTypes.CLOB).create())
                .addColumn(Column.editor().name("VAL_NCLOB").jdbcType(OracleTypes.NCLOB).create())
                .addColumn(Column.editor().name("VAL_BLOB").jdbcType(OracleTypes.BLOB).create())
                .addColumn(Column.editor().name("VAL_CLOB2").jdbcType(OracleTypes.CLOB).create())
                .addColumn(Column.editor().name("VAL_BLOB2").jdbcType(OracleTypes.BLOB).create())
                .create();

        // test unchanged column values in update with both supplied & unsupported lob fields
        String sql = "update \"DEBEZIUM\".\"TEST\" set \"COL1\" = 'Test', \"VAL_CLOB2\" = 'X', \"VAL_BLOB2\" = HEXTORAW('0E') where \"ID\" = '1';";
        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.UPDATE);
        assertThat(entry.getOldValues()).hasSize(7);
        assertThat(entry.getOldValues()[0]).isEqualTo("1");
        assertThat(entry.getOldValues()[1]).isNull();
        assertThat(entry.getOldValues()[2]).isEqualTo(OracleValueConverters.UNAVAILABLE_VALUE);
        assertThat(entry.getOldValues()[3]).isEqualTo(OracleValueConverters.UNAVAILABLE_VALUE);
        assertThat(entry.getOldValues()[4]).isEqualTo(OracleValueConverters.UNAVAILABLE_VALUE);
        assertThat(entry.getOldValues()[5]).isEqualTo(OracleValueConverters.UNAVAILABLE_VALUE);
        assertThat(entry.getOldValues()[6]).isEqualTo(OracleValueConverters.UNAVAILABLE_VALUE);
        assertThat(entry.getNewValues()).hasSize(7);
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
        assertThat(entry.getNewValues()[1]).isEqualTo("Test");
        assertThat(entry.getNewValues()[2]).isEqualTo(OracleValueConverters.UNAVAILABLE_VALUE);
        assertThat(entry.getNewValues()[3]).isEqualTo(OracleValueConverters.UNAVAILABLE_VALUE);
        assertThat(entry.getNewValues()[4]).isEqualTo(OracleValueConverters.UNAVAILABLE_VALUE);
        assertThat(entry.getNewValues()[5]).isEqualTo("X");
        assertThat(entry.getNewValues()[6]).isEqualTo("HEXTORAW('0E')");

        // test unchanged column values not supplied in delete statements
        sql = "delete from \"DEBEZIUM\".\"TEST\" where \"ID\" = '1';";
        entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.DELETE);
        assertThat(entry.getOldValues()).hasSize(7);
        assertThat(entry.getOldValues()[0]).isEqualTo("1");
        assertThat(entry.getOldValues()[1]).isNull();
        assertThat(entry.getOldValues()[2]).isEqualTo(OracleValueConverters.UNAVAILABLE_VALUE);
        assertThat(entry.getOldValues()[3]).isEqualTo(OracleValueConverters.UNAVAILABLE_VALUE);
        assertThat(entry.getOldValues()[4]).isEqualTo(OracleValueConverters.UNAVAILABLE_VALUE);
        assertThat(entry.getOldValues()[5]).isEqualTo(OracleValueConverters.UNAVAILABLE_VALUE);
        assertThat(entry.getOldValues()[6]).isEqualTo(OracleValueConverters.UNAVAILABLE_VALUE);
        assertThat(entry.getNewValues()).isEmpty();
    }

    @Test
    @FixFor("DBZ-5521")
    public void shouldNotInterpretConcatenationSyntaxInSingleQuotedValuesAsConcatenation() throws Exception {
        final Table table = Table.editor()
                .tableId(new TableId(null, "UNKNOWN", "TABLE"))
                .addColumn(Column.editor().name("COL1").create())
                .addColumn(Column.editor().name("COL2").create())
                .create();

        String sql = "insert into \"UNKNOWN\".\"TABLE\" (\"COL1\",\"COL2\") values ('I||am','test||case');";
        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.INSERT);
        assertThat(entry.getOldValues()).isEmpty();
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0]).isEqualTo("I||am");
        assertThat(entry.getNewValues()[1]).isEqualTo("test||case");

        sql = "update \"UNKNOWN\".\"TABLE\" set \"COL1\" = 'I||am||updated' where \"COL1\" = 'I||am' and \"COL2\" = 'test||case';";
        entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.UPDATE);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0]).isEqualTo("I||am");
        assertThat(entry.getOldValues()[1]).isEqualTo("test||case");
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0]).isEqualTo("I||am||updated");
        assertThat(entry.getNewValues()[1]).isEqualTo("test||case");

        sql = "delete from \"UNKNOWN\".\"TABLE\" where \"COL1\" = 'I||am' and \"COL2\" = 'test||case';";
        entry = fastDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.DELETE);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0]).isEqualTo("I||am");
        assertThat(entry.getOldValues()[1]).isEqualTo("test||case");
        assertThat(entry.getNewValues()).isEmpty();
    }

    @Test
    @FixFor("DBZ-3401")
    public void shouldParseInsertOnSchemaVersionMismatch() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.DBZ3401"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("DATA").create())
                .create();

        // Test columns in forward order
        String sql = "insert into \"DEBEZIUM\".\"DBZ3401\" (\"COL 1\",\"COL 2\") values (HEXTORAW('a'),HEXTORAW('b'));";
        LogMinerDmlEntry entry = columnResolverDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.INSERT);
        assertThat(entry.getOldValues()).isEmpty();
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0]).isEqualTo("HEXTORAW('a')");
        assertThat(entry.getNewValues()[1]).isEqualTo("HEXTORAW('b')");

        // Test columns in reverse order
        sql = "insert into \"DEBEZIUM\".\"DBZ3401\" (\"COL 2\",\"COL 1\") values (HEXTORAW('b'),HEXTORAW('a'));";
        entry = columnResolverDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.INSERT);
        assertThat(entry.getOldValues()).isEmpty();
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0]).isEqualTo("HEXTORAW('a')");
        assertThat(entry.getNewValues()[1]).isEqualTo("HEXTORAW('b')");
    }

    @Test
    @FixFor("DBZ-3401")
    public void shouldParseUpdateOnSchemaVersionMismatch() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.DBZ3401"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("DATA").create())
                .create();

        // Test columns in forward order
        String sql = "update \"DEBEZIUM\".\"DBZ3401\" set \"COL 1\" = HEXTORAW('c'), \"COL 2\" = HEXTORAW('d') where \"COL 1\" = HEXTORAW('a') and \"COL 2\" = HEXTORAW('b');";
        LogMinerDmlEntry entry = columnResolverDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.UPDATE);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0]).isEqualTo("HEXTORAW('a')");
        assertThat(entry.getOldValues()[1]).isEqualTo("HEXTORAW('b')");
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0]).isEqualTo("HEXTORAW('c')");
        assertThat(entry.getNewValues()[1]).isEqualTo("HEXTORAW('d')");

        // Test columns in reverse order
        sql = "update \"DEBEZIUM\".\"DBZ3401\" set \"COL 2\" = HEXTORAW('d'), \"COL 1\" = HEXTORAW('c') where \"COL 2\" = HEXTORAW('b') and \"COL 1\" = HEXTORAW('a');";
        entry = columnResolverDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.UPDATE);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0]).isEqualTo("HEXTORAW('a')");
        assertThat(entry.getOldValues()[1]).isEqualTo("HEXTORAW('b')");
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0]).isEqualTo("HEXTORAW('c')");
        assertThat(entry.getNewValues()[1]).isEqualTo("HEXTORAW('d')");
    }

    @Test
    @FixFor("DBZ-3401")
    public void shouldParseDeleteOnSchemaVersionMismatch() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.DBZ3401"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("DATA").create())
                .create();

        // Test where columns in forward order
        String sql = "delete from \"DEBEZIUM\".\"DBZ3401\" where \"COL 1\" = HEXTORAW('a') and \"COL 2\" = HEXTORAW('b');";
        LogMinerDmlEntry entry = columnResolverDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.DELETE);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0]).isEqualTo("HEXTORAW('a')");
        assertThat(entry.getOldValues()[1]).isEqualTo("HEXTORAW('b')");
        assertThat(entry.getNewValues()).isEmpty();

        // Test where columns in reverse order
        sql = "delete from \"DEBEZIUM\".\"DBZ3401\" where \"COL 2\" = HEXTORAW('b') and \"COL 1\" = HEXTORAW('a');";
        entry = columnResolverDmlParser.parse(sql, table);
        assertThat(entry.getEventType()).isEqualTo(EventType.DELETE);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0]).isEqualTo("HEXTORAW('a')");
        assertThat(entry.getOldValues()[1]).isEqualTo("HEXTORAW('b')");
        assertThat(entry.getNewValues()).isEmpty();
    }
}
