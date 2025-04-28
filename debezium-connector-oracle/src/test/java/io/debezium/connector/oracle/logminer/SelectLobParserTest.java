/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.SelectLobParser;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Unit tests for the Oracle LogMiner {@code SEL_LOB_LOCATOR} operation parser, {@link SelectLobParser}.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER)
public class SelectLobParserTest {

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    private static SelectLobParser parser = new SelectLobParser();

    @Test
    @FixFor("DBZ-2948")
    public void shouldParseSimpleClobBasedLobSelect() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.CLOB_TEST"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("VAL_DATA").create())
                .addColumn(Column.editor().name("VAL_CLOB").create())
                .create();

        String redoSql = "DECLARE \n" +
                " loc_c CLOB; \n" +
                " buf_c VARCHAR2(6174); \n" +
                " loc_b BLOB; \n" +
                " buf_b RAW(6174); \n" +
                " loc_nc NCLOB; \n" +
                " buf_nc NVARCHAR2(6174); \n" +
                "BEGIN\n" +
                " select \"VAL_CLOB\" into loc_c from \"DEBEZIUM\".\"CLOB_TEST\" where \"ID\" = '2' and \"VAL_DATA\" = 'Test2' for update;";

        LogMinerDmlEntry entry = parser.parse(redoSql, table);

        assertThat(parser.isBinary()).isFalse();
        assertThat(parser.getColumnName()).isEqualTo("VAL_CLOB");

        assertThat(entry.getObjectOwner()).isEqualTo("DEBEZIUM");
        assertThat(entry.getObjectName()).isEqualTo("CLOB_TEST");
        assertThat(entry.getEventType()).isEqualTo(EventType.SELECT_LOB_LOCATOR);
        assertThat(entry.getOldValues()).hasSize(3);
        assertThat(entry.getOldValues()[0]).isEqualTo("2");
        assertThat(entry.getOldValues()[1]).isEqualTo("Test2");
        assertThat(entry.getNewValues()).hasSize(3);
        assertThat(entry.getNewValues()[0]).isEqualTo("2");
        assertThat(entry.getNewValues()[1]).isEqualTo("Test2");
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldParseSimpleBlobBasedLobSelect() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.BLOB_TEST"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("VAL_DATA").create())
                .addColumn(Column.editor().name("VAL_BLOB").create())
                .create();

        String redoSql = "DECLARE \n" +
                " loc_c CLOB; \n" +
                " buf_c VARCHAR2(6174); \n" +
                " loc_b BLOB; \n" +
                " buf_b RAW(6174); \n" +
                " loc_nc NCLOB; \n" +
                " buf_nc NVARCHAR2(6174); \n" +
                "BEGIN\n" +
                " select \"VAL_BLOB\" into loc_b from \"DEBEZIUM\".\"BLOB_TEST\" where \"ID\" = '2' and \"VAL_DATA\" = 'Test2' for update;";

        LogMinerDmlEntry entry = parser.parse(redoSql, table);

        assertThat(parser.isBinary()).isTrue();
        assertThat(parser.getColumnName()).isEqualTo("VAL_BLOB");

        assertThat(entry.getObjectOwner()).isEqualTo("DEBEZIUM");
        assertThat(entry.getObjectName()).isEqualTo("BLOB_TEST");
        assertThat(entry.getEventType()).isEqualTo(EventType.SELECT_LOB_LOCATOR);
        assertThat(entry.getOldValues()).hasSize(3);
        assertThat(entry.getOldValues()[0]).isEqualTo("2");
        assertThat(entry.getOldValues()[1]).isEqualTo("Test2");
        assertThat(entry.getNewValues()).hasSize(3);
        assertThat(entry.getNewValues()[0]).isEqualTo("2");
        assertThat(entry.getNewValues()[1]).isEqualTo("Test2");
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldParseComplexClobBasedLobSelect() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.BIG_TABLE"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("NAME").create())
                .addColumn(Column.editor().name("AGE").create())
                .addColumn(Column.editor().name("ADRESS").create())
                .addColumn(Column.editor().name("TD").create())
                .addColumn(Column.editor().name("FLAG").create())
                .addColumn(Column.editor().name("CLOB_COL").create())
                .create();

        String redoSql = "DECLARE \n" +
                " loc_c CLOB; \n" +
                " buf_c VARCHAR2(6426); \n" +
                " loc_b BLOB; \n" +
                " buf_b RAW(6426); \n" +
                " loc_nc NCLOB; \n" +
                " buf_nc NVARCHAR2(6426); \n" +
                "BEGIN\n" +
                " select \"CLOB_COL\" into loc_c from \"DEBEZIUM\".\"BIG_TABLE\" where \"ID\" = '651900002' and \"NAME\" = " +
                "'person number 651900002' and \"AGE\" = '125' and \"ADRESS\" = 'street:651900002 av: 651900002 house: 651900002'" +
                " and \"TD\" = TO_DATE('15-MAY-21', 'DD-MON-RR') and \"FLAG\" IS NULL for update;";

        LogMinerDmlEntry entry = parser.parse(redoSql, table);

        assertThat(parser.isBinary()).isFalse();
        assertThat(parser.getColumnName()).isEqualTo("CLOB_COL");

        assertThat(entry.getObjectOwner()).isEqualTo("DEBEZIUM");
        assertThat(entry.getObjectName()).isEqualTo("BIG_TABLE");
        assertThat(entry.getEventType()).isEqualTo(EventType.SELECT_LOB_LOCATOR);
        assertThat(entry.getOldValues()).hasSize(7);
        assertThat(entry.getOldValues()[0]).isEqualTo("651900002");
        assertThat(entry.getOldValues()[1]).isEqualTo("person number 651900002");
        assertThat(entry.getOldValues()[2]).isEqualTo("125");
        assertThat(entry.getOldValues()[3]).isEqualTo("street:651900002 av: 651900002 house: 651900002");
        assertThat(entry.getOldValues()[4]).isEqualTo("TO_DATE('15-MAY-21', 'DD-MON-RR')");
        assertThat(entry.getOldValues()[5]).isNull();
        assertThat(entry.getOldValues()).hasSize(7);
        assertThat(entry.getNewValues()[0]).isEqualTo("651900002");
        assertThat(entry.getNewValues()[1]).isEqualTo("person number 651900002");
        assertThat(entry.getNewValues()[2]).isEqualTo("125");
        assertThat(entry.getNewValues()[3]).isEqualTo("street:651900002 av: 651900002 house: 651900002");
        assertThat(entry.getNewValues()[4]).isEqualTo("TO_DATE('15-MAY-21', 'DD-MON-RR')");
        assertThat(entry.getNewValues()[5]).isNull();
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldParseComplexBlobBasedLobSelect() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.BIG_TABLE"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("NAME").create())
                .addColumn(Column.editor().name("AGE").create())
                .addColumn(Column.editor().name("ADRESS").create())
                .addColumn(Column.editor().name("TD").create())
                .addColumn(Column.editor().name("FLAG").create())
                .addColumn(Column.editor().name("BLOB_COL").create())
                .create();

        String redoSql = "DECLARE \n" +
                " loc_c CLOB; \n" +
                " buf_c VARCHAR2(6426); \n" +
                " loc_b BLOB; \n" +
                " buf_b RAW(6426); \n" +
                " loc_nc NCLOB; \n" +
                " buf_nc NVARCHAR2(6426); \n" +
                "BEGIN\n" +
                " select \"BLOB_COL\" into loc_b from \"DEBEZIUM\".\"BIG_TABLE\" where \"ID\" = '651900002' and \"NAME\" = " +
                "'person number 651900002' and \"AGE\" = '125' and \"ADRESS\" = 'street:651900002 av: 651900002 house: 651900002'" +
                " and \"TD\" = TO_DATE('15-MAY-21', 'DD-MON-RR') and \"FLAG\" IS NULL for update;";

        LogMinerDmlEntry entry = parser.parse(redoSql, table);

        assertThat(parser.isBinary()).isTrue();
        assertThat(parser.getColumnName()).isEqualTo("BLOB_COL");

        assertThat(entry.getObjectOwner()).isEqualTo("DEBEZIUM");
        assertThat(entry.getObjectName()).isEqualTo("BIG_TABLE");
        assertThat(entry.getEventType()).isEqualTo(EventType.SELECT_LOB_LOCATOR);
        assertThat(entry.getOldValues()).hasSize(7);
        assertThat(entry.getOldValues()[0]).isEqualTo("651900002");
        assertThat(entry.getOldValues()[1]).isEqualTo("person number 651900002");
        assertThat(entry.getOldValues()[2]).isEqualTo("125");
        assertThat(entry.getOldValues()[3]).isEqualTo("street:651900002 av: 651900002 house: 651900002");
        assertThat(entry.getOldValues()[4]).isEqualTo("TO_DATE('15-MAY-21', 'DD-MON-RR')");
        assertThat(entry.getOldValues()[5]).isNull();
        assertThat(entry.getOldValues()).hasSize(7);
        assertThat(entry.getNewValues()[0]).isEqualTo("651900002");
        assertThat(entry.getNewValues()[1]).isEqualTo("person number 651900002");
        assertThat(entry.getNewValues()[2]).isEqualTo("125");
        assertThat(entry.getNewValues()[3]).isEqualTo("street:651900002 av: 651900002 house: 651900002");
        assertThat(entry.getNewValues()[4]).isEqualTo("TO_DATE('15-MAY-21', 'DD-MON-RR')");
        assertThat(entry.getNewValues()[5]).isNull();
    }

    @Test
    @FixFor("DBZ-4994")
    public void shouldParseColumnWithEscapedSingleQuoteColumnValues() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.QUOTE_TABLE"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("NAME").create())
                .addColumn(Column.editor().name("CLOB_COL").create())
                .create();

        String redoSql = "DECLARE \n" +
                " loc_c CLOB; \n" +
                " buf_c VARCHAR2(6426); \n" +
                " loc_b BLOB; \n" +
                " buf_b RAW(6426); \n" +
                " loc_nc NCLOB; \n" +
                " buf_nc NVARCHAR2(6426); \n" +
                "BEGIN\n" +
                " select \"CLOB_COL\" into loc_c from \"DEBEZIUM\".\"QUOTE_TABLE\" where \"ID\" = '1' and \"NAME\" = " +
                "'2\"''\" sd f\"\"\" '''''''' ''''' for update;";

        LogMinerDmlEntry entry = parser.parse(redoSql, table);

        assertThat(parser.isBinary()).isFalse();
        assertThat(parser.getColumnName()).isEqualTo("CLOB_COL");

        assertThat(entry.getObjectOwner()).isEqualTo("DEBEZIUM");
        assertThat(entry.getObjectName()).isEqualTo("QUOTE_TABLE");
        assertThat(entry.getEventType()).isEqualTo(EventType.SELECT_LOB_LOCATOR);
        assertThat(entry.getOldValues()).hasSize(3);
        assertThat(entry.getOldValues()[0]).isEqualTo("1");
        assertThat(entry.getOldValues()[1]).isEqualTo("2\"''\" sd f\"\"\" '''''''' ''''");
        assertThat(entry.getOldValues()[2]).isNull();
        assertThat(entry.getNewValues()).hasSize(3);
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
        assertThat(entry.getNewValues()[1]).isEqualTo("2\"''\" sd f\"\"\" '''''''' ''''");
        assertThat(entry.getNewValues()[2]).isNull();
    }
}
