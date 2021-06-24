/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName;
import io.debezium.connector.oracle.logminer.parser.DmlParser;
import io.debezium.connector.oracle.logminer.parser.DmlParserException;
import io.debezium.connector.oracle.logminer.parser.SimpleDmlParser;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.IoUtil;

import net.sf.jsqlparser.statement.update.Update;

/**
 * This is the test suite for Oracle Antlr and jsqlparser DML parser unit testing
 */
@SkipWhenAdapterNameIsNot(value = AdapterName.LOGMINER)
public class OracleDmlParserTest {

    private OracleDdlParser ddlParser;
    private SimpleDmlParser sqlDmlParser;
    private Tables tables;
    private static final String TABLE_NAME = "TEST";
    private static final String CATALOG_NAME = "ORCLPDB1";
    private static final String SCHEMA_NAME = "DEBEZIUM";
    private static final String FULL_TABLE_NAME = SCHEMA_NAME + "\".\"" + TABLE_NAME;
    private static final TableId TABLE_ID = TableId.parse(CATALOG_NAME + "." + SCHEMA_NAME + "." + TABLE_NAME);
    private static final String SPATIAL_DATA = "SDO_GEOMETRY(2003, NULL, NULL, SDO_ELEM_INFO_ARRAY(1, 1003, 1), SDO_ORDINATE_ARRAY" +
            "(102604.878, 85772.8286, 101994.879, 85773.6633, 101992.739, 84209.6648, 102602.738, 84208.83, 102604.878, 85772.8286))";
    private static final String SPATIAL_DATA_1 = "'unsupported type'";
    private static String CLOB_DATA;
    private static byte[] BLOB_DATA; // todo

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    @Before
    public void setUp() {
        OracleValueConverters converters = new OracleValueConverters(new OracleConnectorConfig(TestHelper.defaultConfig().build()), null);

        ddlParser = new OracleDdlParser();
        ddlParser.setCurrentSchema(SCHEMA_NAME);
        ddlParser.setCurrentDatabase(CATALOG_NAME);
        sqlDmlParser = new SimpleDmlParser(CATALOG_NAME, converters);
        tables = new Tables();

        CLOB_DATA = StringUtils.repeat("clob_", 4000);
        String blobString = "blob_";
        BLOB_DATA = Arrays.copyOf(blobString.getBytes(), 8000); // todo doesn't support blob

    }

    @Test
    public void shouldParseAliasUpdate() throws Exception {
        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);

        String dml = "update \"" + FULL_TABLE_NAME + "\" a set a.\"col1\" = '9', a.col2 = 'diFFerent', a.col3 = 'anotheR', a.col4 = '123', a.col6 = 5.2, " +
                "a.col8 = TO_TIMESTAMP('2019-05-14 02:28:32.302000'), a.col10 = " + CLOB_DATA + ", a.col11 = null, a.col12 = '1', " +
                "a.col7 = TO_DATE('2018-02-22 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), " +
                "a.col13 = TO_DATE('2018-02-22 00:00:00', 'YYYY-MM-DD HH24:MI:SS') " +
                "where a.ID = 5 and a.COL1 = 6 and a.\"COL2\" = 'text' " +
                "and a.COL3 = 'text' and a.COL4 IS NULL and a.\"COL5\" IS NULL and a.COL6 IS NULL " +
                "and a.COL8 = TO_TIMESTAMP('2019-05-14 02:28:32.') and a.col11 is null;";

        LogMinerDmlEntry record = sqlDmlParser.parse(dml, tables.forTable(TABLE_ID), "1");
        verifyUpdate(record, false, true, 14);
    }

    @Test
    public void shouldParseTimestampFormats() throws Exception {
        Locale defaultLocale = Locale.getDefault();
        try {
            String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
            ddlParser.parse(createStatement, tables);
            String format1 = "TO_TIMESTAMP('2020-09-22 00:09:37.302000')";
            String format2 = "TO_TIMESTAMP('2020-09-22 00:09:37.')";
            String format3 = "TO_TIMESTAMP('2020-09-22 00:09:37')";
            String format4 = "TO_TIMESTAMP('22-SEP-20 12.09.37 AM')";
            String format5 = "TO_TIMESTAMP('22-SEP-20 12.09.37 PM')";
            String format6 = "TO_TIMESTAMP('29-SEP-20 06.02.24.777000 PM')";
            String format7 = "TO_TIMESTAMP('2020-09-22 00:09:37.0')";

            parseTimestamp(format1, false);
            parseTimestamp(format2, true);
            parseTimestamp(format3, true);
            // Change to locale that does not recognize SEP in pattern MMM
            Locale.setDefault(Locale.GERMAN);
            parseTimestamp(format4, true);
            parseTimestamp(format5, false);
            parseTimestamp(format6, false);
            Locale.setDefault(defaultLocale);
            parseTimestamp(format7, true);
        }
        finally {
            Locale.setDefault(defaultLocale);
        }
    }

    @Test
    @FixFor("DBZ-2784")
    public void shouldParseDateFormats() throws Exception {
        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);

        String format1 = "TO_DATE('2018-02-22 00:00:00', 'YYYY-MM-DD HH24:MI:SS')";
        parseDate(format1, true);
    }

    private void parseDate(String format, boolean validateDate) {
        String dml = "update \"" + FULL_TABLE_NAME + "\" a set a.\"col7\" = " + format + ", a.\"col13\" = " + format + " where a.ID = 1;";

        LogMinerDmlEntry record = sqlDmlParser.parse(dml, tables.forTable(TABLE_ID), "1");
        assertThat(record).isNotNull();
        assertThat(record.getNewValues()).isNotEmpty();
        assertThat(record.getOldValues()).isNotEmpty();

        if (validateDate) {
            assertThat(record.getNewValues()[7]).isEqualTo(1519257600000L);
            assertThat(record.getNewValues()[13]).isEqualTo(1519257600000L);
        }
    }

    private void parseTimestamp(String format, boolean validateTimestamp) {
        String dml = "update \"" + FULL_TABLE_NAME + "\" a set a.\"col1\" = '9', a.col2 = 'diFFerent', a.col3 = 'anotheR', a.col4 = '123', a.col6 = 5.2, " +
                "a.col8 = " + format + ", a.col10 = " + CLOB_DATA + ", a.col11 = null, a.col12 = '1' " +
                "where a.ID = 5 and a.COL1 = 6 and a.\"COL2\" = 'text' " +
                "and a.COL3 = 'text' and a.COL4 IS NULL and a.\"COL5\" IS NULL and a.COL6 IS NULL " +
                "and a.COL8 = " + format + " and a.col11 is null;";

        LogMinerDmlEntry record = sqlDmlParser.parse(dml, tables.forTable(TABLE_ID), "1");
        assertThat(record.getNewValues()).isNotEmpty();
        assertThat(record.getOldValues()).isNotEmpty();

        if (validateTimestamp) {
            assertThat(record.getNewValues()[8]).isEqualTo(1600733377000000L);
        }
    }

    @Test
    public void shouldParseAliasInsert() throws Exception {
        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);

        String dml = "insert into \"" + FULL_TABLE_NAME + "\" a (a.\"ID\",a.\"COL1\",a.\"COL2\",a.\"COL3\",a.\"COL4\",a.\"COL5\",a.\"COL6\",a.\"COL8\"," +
                "a.\"COL9\",a.\"COL10\",a.\"COL13\") values ('5','4','tExt','text',NULL,NULL,NULL,NULL,EMPTY_BLOB(),EMPTY_CLOB(),TO_DATE('2018-02-22 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));";

        LogMinerDmlEntry record = sqlDmlParser.parse(dml, tables.forTable(TABLE_ID), "1");
        verifyInsert(record);
    }

    @Test
    public void shouldParseAliasDelete() throws Exception {
        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);

        String dml = "delete from \"" + FULL_TABLE_NAME +
                "\" a where a.\"id\" = 6 and a.\"col1\" = 2 and a.\"col2\" = 'text' and a.col3 = 'tExt' and a.col4 is null and a.col5 is null " +
                " and a.col6 is null and a.col8 is null and a.col9 is null and a.col10 is null and a.col11 is null and a.col12 is null";

        LogMinerDmlEntry record = sqlDmlParser.parse(dml, tables.forTable(TABLE_ID), "1");
        verifyDelete(record, true);
    }

    @Test
    public void shouldParseNoWhereClause() throws Exception {
        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);

        String dml = "update \"" + FULL_TABLE_NAME
                + "\" a set a.\"id\"=1, a.\"col1\" = '9', a.col2 = 'diFFerent', a.col3 = 'anotheR', a.col4 = '123', a.col5 = null, a.col6 = 5.2, " +
                "a.col8 = TO_TIMESTAMP('2019-05-14 02:28:32.302000'), a.col9=null, a.col10 = " + CLOB_DATA + ", a.col11 = null, a.col12 = '1', " +
                "a.col7 = TO_DATE('2018-02-22 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), " +
                "a.col13 = TO_DATE('2018-02-22 00:00:00', 'YYYY-MM-DD HH24:MI:SS')";

        LogMinerDmlEntry record = sqlDmlParser.parse(dml, tables.forTable(TABLE_ID), "1");
        verifyUpdate(record, false, false, 9);

        dml = "delete from \"" + FULL_TABLE_NAME + "\" a ";

        record = sqlDmlParser.parse(dml, tables.forTable(TABLE_ID), "1");
        verifyDelete(record, false);
    }

    @Test
    public void shouldParseInsertAndDeleteTable() throws Exception {

        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);

        String dml = "insert into \"" + FULL_TABLE_NAME + "\"(\"ID\",\"COL1\",\"COL2\",\"COL3\",\"COL4\",\"COL5\",\"COL6\",\"COL8\"," +
                "\"COL9\",\"COL10\") values ('5','4','tExt','text',NULL,NULL,NULL,NULL,EMPTY_BLOB(),EMPTY_CLOB());";

        LogMinerDmlEntry record = sqlDmlParser.parse(dml, tables.forTable(TABLE_ID), "1");
        verifyInsert(record);

        dml = "delete from \"" + FULL_TABLE_NAME +
                "\" where id = 6 and col1 = 2 and col2 = 'text' and col3 = 'tExt' and col4 is null and col5 is null " +
                " and col6 is null and col8 is null and col9 is null and col10 is null and col11 is null and col12 is null";

        record = sqlDmlParser.parse(dml, tables.forTable(TABLE_ID), "");
        verifyDelete(record, true);
    }

    // todo encrypted columns and spatial will be represented as "Unsupported Type"
    @Test
    public void shouldParseUpdateTable() throws Exception {

        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);

        String dml = "update \"" + FULL_TABLE_NAME + "\" set \"col1\" = '9', col2 = 'diFFerent', col3 = 'anotheR', col4 = '123', col6 = '5.2', " +
                "col8 = TO_TIMESTAMP('2019-05-14 02:28:32.302000'), col10='clob_', col12 = '1' " +
                "where ID = 5 and COL1 = 6 and \"COL2\" = 'text' " +
                "and COL3 = 'text' and COL4 IS NULL and \"COL5\" IS NULL and COL6 IS NULL AND COL7 = TO_DATE('2018-02-22 00:00:00', 'YYYY-MM-DD HH24:MI:SS') " +
                "and COL8 = TO_TIMESTAMP('2019-05-14 02:28:32') and col11 = " + SPATIAL_DATA + " and COL13 = TO_DATE('2018-02-22 00:00:00', 'YYYY-MM-DD HH24:MI:SS');";

        LogMinerDmlEntry record = sqlDmlParser.parse(dml, tables.forTable(TABLE_ID), "");
        verifyUpdate(record, true, true, 14);

        dml = "update \"" + FULL_TABLE_NAME
                + "\" set \"col1\" = '9', col2 = '$2a$10$aHo.lQk.YAkGl5AkXbjJhODBqwNLkqF94slP5oZ3boNzm0d04WnE2', col3 = NULL, col4 = '123', col6 = '5.2', " +
                "col8 = TO_TIMESTAMP('2019-05-14 02:28:32.302000'), col10='clob_', col12 = '1' " +
                "where ID = 5 and COL1 = 6 and \"COL2\" = 'johan.philtjens@dpworld.com' " +
                "and COL3 = 'text' and COL4 IS NULL and \"COL5\" IS NULL and COL6 IS NULL " +
                "and COL8 = TO_TIMESTAMP('2019-05-14 02:28:32') and col11 = " + SPATIAL_DATA + ";";

        record = sqlDmlParser.parse(dml, tables.forTable(TABLE_ID), "");
    }

    @Test
    public void shouldParseUpdateNoChangesTable() throws Exception {

        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);

        String dml = "update \"" + FULL_TABLE_NAME + "\" set \"col1\" = '6', col2 = 'text', col3 = 'text', col4 = NULL " +
                "where ID = 5 and COL1 = 6 and \"COL2\" = 'text' " +
                "and COL3 = Unsupported Type and COL4 IS NULL and \"COL5\" IS NULL and COL6 IS NULL and COL7 IS NULL and COL9 IS NULL and COL10 IS NULL and COL12 IS NULL "
                +
                "and COL8 = TO_TIMESTAMP('2019-05-14 02:28:32') and col11 = " + SPATIAL_DATA + ";";

        LogMinerDmlEntry record = sqlDmlParser.parse(dml, tables.forTable(TABLE_ID), "");
        boolean pass = record.getOperation() == RowMapper.UPDATE
                && record.getOldValues().length == record.getNewValues().length
                && Objects.equals(record.getNewValues(), record.getOldValues());
        assertThat(pass);
        assertThat(record.getOldValues()[4]).isNull();
    }

    @Test
    public void shouldParseSpecialCharacters() throws Exception {

        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);

        String dml = "insert into \"" + FULL_TABLE_NAME + "\"(\"ID\",\"COL1\",\"COL2\",\"COL3\",\"COL4\",\"COL5\",\"COL6\",\"COL8\"," +
                "\"COL9\",\"COL10\") values ('5','4','\\','\\test',NULL,NULL,NULL,NULL,EMPTY_BLOB(),EMPTY_CLOB());";

        LogMinerDmlEntry result = sqlDmlParser.parse(dml, tables.forTable(TABLE_ID), "1");
        assertThat(result).isNotNull();
        assertThat(result.getNewValues()[2].toString().contains("\\"));

        dml = "delete from \"" + FULL_TABLE_NAME +
                "\" where id = 6 and col1 = 2 and col2 = 'te\\xt' and col3 = 'tExt\\' and col4 is null and col5 is null " +
                " and col6 is null and col8 is null and col9 is null and col10 is null and col11 is null and col12 is null";

        result = sqlDmlParser.parse(dml, tables.forTable(TABLE_ID), "");
        assertThat(result).isNotNull();
        assertThat(result.getOldValues()[3].toString().contains("\\"));
    }

    @Test
    public void shouldParseStrangeDml() throws Exception {
        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);
        String dml = null;
        LogMinerDmlEntry result = sqlDmlParser.parse(dml, tables.forTable(TABLE_ID), "");
        assertThat(result).isNull();
        dml = "select * from test;null;";
        assertDmlParserException(dml, sqlDmlParser, tables.forTable(TABLE_ID), "");
        assertThat(result).isNull();
        dml = "full dummy mess";
        assertDmlParserException(dml, sqlDmlParser, tables.forTable(TABLE_ID), "");
        dml = "delete from non_exiting_table " +
                " where id = 6 and col1 = 2 and col2 = 'te\\xt' and col3 = 'tExt\\' and col4 is null and col5 is null " +
                " and col6 is null and col8 is null and col9 is null and col10 is null and col11 is null and col12 is null";
        assertDmlParserException(dml, sqlDmlParser, tables.forTable(TABLE_ID), "");

        Update update = mock(Update.class);
        Mockito.when(update.getTables()).thenReturn(new ArrayList<>());
        dml = "update  \"" + FULL_TABLE_NAME + "\" set col1 = 3 " +
                " where id = 6 and col1 = 2 and col2 = 'te\\xt' and col3 = 'tExt\\' and col4 is null and col5 is null " +
                " and col6 is null and col8 is null and col9 is null and col10 is null and col11 is null and col12 is null and col20 is null";
        result = sqlDmlParser.parse(dml, tables.forTable(TABLE_ID), "");
        assertThat(result.getOldValues()).hasSize(14);

        dml = "update \"" + FULL_TABLE_NAME + "\" set col1 = 3 " +
                " where id = 6 and col1 = 2 and col2 = 'te\\xt' and col30 = 'tExt\\' and col4 is null and col5 is null " +
                " and col6 is null and col8 is null and col9 is null and col10 is null and col11 is null and col21 is null";
        result = sqlDmlParser.parse(dml, tables.forTable(TABLE_ID), "");
        assertThat(result.getNewValues()).hasSize(14);

        dml = "update table1, \"" + FULL_TABLE_NAME + "\" set col1 = 3 " +
                " where id = 6 and col1 = 2 and col2 = 'te\\xt' and col3 = 'tExt\\' and col4 is null and col5 is null " +
                " and col6 is null and col8 is null and col9 is null and col10 is null and col11 is null and col12 is null and col20 is null";
        assertDmlParserException(dml, sqlDmlParser, tables.forTable(TABLE_ID), "");
    }

    private void assertDmlParserException(String sql, DmlParser parser, Table table, String txId) {
        try {
            LogMinerDmlEntry dml = parser.parse(sql, table, txId);
        }
        catch (Exception e) {
            assertThat(e).isInstanceOf(DmlParserException.class);
        }
    }

    private void verifyUpdate(LogMinerDmlEntry record, boolean checkGeometry, boolean checkOldValues, int oldValuesNumber) {
        // validate
        assertThat(record.getOperation()).isEqualTo(RowMapper.UPDATE);
        assertThat(record.getNewValues()).hasSize(14);

        for (int i = 0; i < record.getNewValues().length; ++i) {
            Object newValue = record.getNewValues()[i];
            switch (i) {
                case 1:
                    assertThat(newValue).isEqualTo(BigDecimal.valueOf(900, 2));
                    break;
                case 2:
                    assertThat(newValue).isEqualTo("diFFerent");
                    break;
                case 3:
                    assertThat(newValue).isEqualTo("anotheR");
                    break;
                case 4:
                    assertThat(newValue).isEqualTo("123");
                    break;
                case 6:
                    assertThat(((Struct) newValue).get("scale")).isEqualTo(1);
                    assertThat(((byte[]) ((Struct) newValue).get("value"))[0]).isEqualTo((byte) 52);
                    break;
                case 7:
                case 13:
                    assertThat(newValue).isInstanceOf(Long.class);
                    assertThat(newValue).isEqualTo(1519257600000L);
                    break;
                case 8:
                    assertThat(newValue).isInstanceOf(Long.class);
                    assertThat(newValue).isEqualTo(1557800912302000L);
                    break;
                case 10:
                    assertThat(newValue).isInstanceOf(String.class);
                    assertThat(newValue.toString().contains("clob_")).isTrue();
                    break;
                case 11:
                    if (checkGeometry) {
                        assertThat(newValue).isInstanceOf(String.class);
                        assertThat(newValue.toString().contains("SDO_GEOMETRY")).isTrue();
                    }
                    else {
                        assertThat(newValue).isNull();
                    }
                    break;
                case 12:
                    assertThat(newValue).isInstanceOf(Byte.class);
                    assertThat(newValue).isEqualTo(Byte.valueOf("1"));
                    break;
            }
        }

        if (!checkOldValues) {
            assertThat(record.getOldValues()).hasSize(0);
        }
        else {
            assertThat(record.getOldValues()).hasSize(oldValuesNumber);
            for (int i = 0; i < record.getOldValues().length; ++i) {
                Object oldValue = record.getOldValues()[i];
                switch (i) {
                    case 1:
                        assertThat(oldValue).isEqualTo(BigDecimal.valueOf(600, 2));
                        break;
                    case 2:
                        assertThat(oldValue).isEqualTo("text");
                        break;
                    case 3:
                        assertThat(oldValue).isEqualTo("text");
                        break;
                    case 4:
                        assertThat(oldValue).isNull();
                        break;
                    case 5:
                        assertThat(oldValue).isNull();
                        break;
                    case 6:
                        assertThat(oldValue).isNull();
                        break;
                    case 8:
                        assertThat(oldValue).isInstanceOf(Long.class);
                        assertThat(oldValue).isEqualTo(1557800912000000L);
                        break;
                    case 11:
                        if (checkGeometry) {
                            assertThat(oldValue).isInstanceOf(String.class);
                            assertThat(oldValue.toString().contains("SDO_GEOMETRY")).isTrue();
                        }
                        else {
                            assertThat(oldValue).isNull();
                        }
                        break;
                    case 0:
                        assertThat(oldValue).isEqualTo(new BigDecimal(5));
                        break;
                }
            }
        }
    }

    private void verifyInsert(LogMinerDmlEntry record) {
        assertThat(record.getOldValues()).hasSize(0);
        assertThat(record.getOperation()).isEqualTo(RowMapper.INSERT);
        assertThat(record.getNewValues()).hasSize(14);

        assertThat(record.getNewValues()[0]).isEqualTo(new BigDecimal(5));
        assertThat(record.getNewValues()[1]).isEqualTo(BigDecimal.valueOf(400, 2));
        assertThat(record.getNewValues()[2]).isEqualTo("tExt");
        assertThat(record.getNewValues()[3]).isEqualTo("text");
        assertThat(record.getNewValues()[4]).isNull();
        assertThat(record.getNewValues()[5]).isNull();
        assertThat(record.getNewValues()[6]).isNull();
        assertThat(record.getNewValues()[7]).isNull();
        // todo handle LOBS
        // assertThat(iterator.next().getColumnData()).isNull();
        // assertThat(iterator.next().getColumnData()).isNull();

    }

    private void verifyDelete(LogMinerDmlEntry record, boolean checkOldValues) {
        assertThat(record.getOperation()).isEqualTo(RowMapper.DELETE);
        assertThat(record.getNewValues()).hasSize(0);

        if (!checkOldValues) {
            assertThat(record.getOldValues()).hasSize(0);
        }
        else {
            assertThat(record.getOldValues()).hasSize(14);

            assertThat(record.getOldValues()[0]).isEqualTo(new BigDecimal(6));
            assertThat(record.getOldValues()[1]).isEqualTo(BigDecimal.valueOf(200, 2));
            assertThat(record.getOldValues()[2]).isEqualTo("text");
            assertThat(record.getOldValues()[3]).isEqualTo("tExt");
            assertThat(record.getOldValues()[4]).isNull();
            assertThat(record.getOldValues()[5]).isNull();
            assertThat(record.getOldValues()[6]).isNull();
            assertThat(record.getOldValues()[7]).isNull();
            assertThat(record.getOldValues()[8]).isNull();
            assertThat(record.getOldValues()[9]).isNull();
            assertThat(record.getOldValues()[10]).isNull();
        }
    }
}
