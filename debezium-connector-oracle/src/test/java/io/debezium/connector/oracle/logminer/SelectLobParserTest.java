/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

import io.debezium.connector.oracle.logminer.parser.SelectLobParser;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.doc.FixFor;

/**
 * Unit tests for the Oracle LogMiner {@code SEL_LOB_LOCATOR} operation parser, {@link SelectLobParser}.
 *
 * @author Chris Cranford
 */
public class SelectLobParserTest {

    private static SelectLobParser parser = new SelectLobParser();

    @Test
    @FixFor("DBZ-2948")
    public void shouldParseSimpleClobBasedLobSelect() throws Exception {
        String redoSql = "DECLARE \n" +
                " loc_c CLOB; \n" +
                " buf_c VARCHAR2(6174); \n" +
                " loc_b BLOB; \n" +
                " buf_b RAW(6174); \n" +
                " loc_nc NCLOB; \n" +
                " buf_nc NVARCHAR2(6174); \n" +
                "BEGIN\n" +
                " select \"VAL_CLOB\" into loc_c from \"DEBEZIUM\".\"CLOB_TEST\" where \"ID\" = '2' and \"VAL_DATA\" = 'Test2' for update;";

        LogMinerDmlEntry entry = parser.parse(redoSql);

        assertThat(parser.isBinary()).isFalse();
        assertThat(parser.getColumnName()).isEqualTo("VAL_CLOB");

        assertThat(entry.getObjectOwner()).isEqualTo("DEBEZIUM");
        assertThat(entry.getObjectName()).isEqualTo("CLOB_TEST");
        assertThat(entry.getOperation()).isEqualTo(RowMapper.SELECT_LOB_LOCATOR);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getOldValues().get(0).getColumnData()).isEqualTo("2");
        assertThat(entry.getOldValues().get(1).getColumnName()).isEqualTo("VAL_DATA");
        assertThat(entry.getOldValues().get(1).getColumnData()).isEqualTo("Test2");
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getNewValues().get(0).getColumnData()).isEqualTo("2");
        assertThat(entry.getNewValues().get(1).getColumnName()).isEqualTo("VAL_DATA");
        assertThat(entry.getNewValues().get(1).getColumnData()).isEqualTo("Test2");
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldParseSimpleBlobBasedLobSelect() throws Exception {
        String redoSql = "DECLARE \n" +
                " loc_c CLOB; \n" +
                " buf_c VARCHAR2(6174); \n" +
                " loc_b BLOB; \n" +
                " buf_b RAW(6174); \n" +
                " loc_nc NCLOB; \n" +
                " buf_nc NVARCHAR2(6174); \n" +
                "BEGIN\n" +
                " select \"VAL_BLOB\" into loc_b from \"DEBEZIUM\".\"BLOB_TEST\" where \"ID\" = '2' and \"VAL_DATA\" = 'Test2' for update;";

        LogMinerDmlEntry entry = parser.parse(redoSql);

        assertThat(parser.isBinary()).isTrue();
        assertThat(parser.getColumnName()).isEqualTo("VAL_BLOB");

        assertThat(entry.getObjectOwner()).isEqualTo("DEBEZIUM");
        assertThat(entry.getObjectName()).isEqualTo("BLOB_TEST");
        assertThat(entry.getOperation()).isEqualTo(RowMapper.SELECT_LOB_LOCATOR);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getOldValues().get(0).getColumnData()).isEqualTo("2");
        assertThat(entry.getOldValues().get(1).getColumnName()).isEqualTo("VAL_DATA");
        assertThat(entry.getOldValues().get(1).getColumnData()).isEqualTo("Test2");
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getNewValues().get(0).getColumnData()).isEqualTo("2");
        assertThat(entry.getNewValues().get(1).getColumnName()).isEqualTo("VAL_DATA");
        assertThat(entry.getNewValues().get(1).getColumnData()).isEqualTo("Test2");
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldParseComplexClobBasedLobSelect() throws Exception {
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
                " and \"TD\" = TO_DATE('15-MAY-21', 'DD-MON-RR') and \"FLAG\" is null for update;";

        LogMinerDmlEntry entry = parser.parse(redoSql);

        assertThat(parser.isBinary()).isFalse();
        assertThat(parser.getColumnName()).isEqualTo("CLOB_COL");

        assertThat(entry.getObjectOwner()).isEqualTo("DEBEZIUM");
        assertThat(entry.getObjectName()).isEqualTo("BIG_TABLE");
        assertThat(entry.getOperation()).isEqualTo(RowMapper.SELECT_LOB_LOCATOR);
        assertThat(entry.getOldValues()).hasSize(6);
        assertThat(entry.getOldValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getOldValues().get(0).getColumnData()).isEqualTo("651900002");
        assertThat(entry.getOldValues().get(1).getColumnName()).isEqualTo("NAME");
        assertThat(entry.getOldValues().get(1).getColumnData()).isEqualTo("person number 651900002");
        assertThat(entry.getOldValues().get(2).getColumnName()).isEqualTo("AGE");
        assertThat(entry.getOldValues().get(2).getColumnData()).isEqualTo("125");
        assertThat(entry.getOldValues().get(3).getColumnName()).isEqualTo("ADRESS");
        assertThat(entry.getOldValues().get(3).getColumnData()).isEqualTo("street:651900002 av: 651900002 house: 651900002");
        assertThat(entry.getOldValues().get(4).getColumnName()).isEqualTo("TD");
        assertThat(entry.getOldValues().get(4).getColumnData()).isEqualTo("TO_DATE('15-MAY-21', 'DD-MON-RR')");
        assertThat(entry.getOldValues().get(5).getColumnName()).isEqualTo("FLAG");
        assertThat(entry.getOldValues().get(5).getColumnData()).isNull();
        assertThat(entry.getOldValues()).hasSize(6);
        assertThat(entry.getNewValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getNewValues().get(0).getColumnData()).isEqualTo("651900002");
        assertThat(entry.getNewValues().get(1).getColumnName()).isEqualTo("NAME");
        assertThat(entry.getNewValues().get(1).getColumnData()).isEqualTo("person number 651900002");
        assertThat(entry.getNewValues().get(2).getColumnName()).isEqualTo("AGE");
        assertThat(entry.getNewValues().get(2).getColumnData()).isEqualTo("125");
        assertThat(entry.getNewValues().get(3).getColumnName()).isEqualTo("ADRESS");
        assertThat(entry.getNewValues().get(3).getColumnData()).isEqualTo("street:651900002 av: 651900002 house: 651900002");
        assertThat(entry.getNewValues().get(4).getColumnName()).isEqualTo("TD");
        assertThat(entry.getNewValues().get(4).getColumnData()).isEqualTo("TO_DATE('15-MAY-21', 'DD-MON-RR')");
        assertThat(entry.getNewValues().get(5).getColumnName()).isEqualTo("FLAG");
        assertThat(entry.getNewValues().get(5).getColumnData()).isNull();
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldParseComplexBlobBasedLobSelect() throws Exception {
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
                " and \"TD\" = TO_DATE('15-MAY-21', 'DD-MON-RR') and \"FLAG\" is null for update;";

        LogMinerDmlEntry entry = parser.parse(redoSql);

        assertThat(parser.isBinary()).isTrue();
        assertThat(parser.getColumnName()).isEqualTo("BLOB_COL");

        assertThat(entry.getObjectOwner()).isEqualTo("DEBEZIUM");
        assertThat(entry.getObjectName()).isEqualTo("BIG_TABLE");
        assertThat(entry.getOperation()).isEqualTo(RowMapper.SELECT_LOB_LOCATOR);
        assertThat(entry.getOldValues()).hasSize(6);
        assertThat(entry.getOldValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getOldValues().get(0).getColumnData()).isEqualTo("651900002");
        assertThat(entry.getOldValues().get(1).getColumnName()).isEqualTo("NAME");
        assertThat(entry.getOldValues().get(1).getColumnData()).isEqualTo("person number 651900002");
        assertThat(entry.getOldValues().get(2).getColumnName()).isEqualTo("AGE");
        assertThat(entry.getOldValues().get(2).getColumnData()).isEqualTo("125");
        assertThat(entry.getOldValues().get(3).getColumnName()).isEqualTo("ADRESS");
        assertThat(entry.getOldValues().get(3).getColumnData()).isEqualTo("street:651900002 av: 651900002 house: 651900002");
        assertThat(entry.getOldValues().get(4).getColumnName()).isEqualTo("TD");
        assertThat(entry.getOldValues().get(4).getColumnData()).isEqualTo("TO_DATE('15-MAY-21', 'DD-MON-RR')");
        assertThat(entry.getOldValues().get(5).getColumnName()).isEqualTo("FLAG");
        assertThat(entry.getOldValues().get(5).getColumnData()).isNull();
        assertThat(entry.getOldValues()).hasSize(6);
        assertThat(entry.getNewValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getNewValues().get(0).getColumnData()).isEqualTo("651900002");
        assertThat(entry.getNewValues().get(1).getColumnName()).isEqualTo("NAME");
        assertThat(entry.getNewValues().get(1).getColumnData()).isEqualTo("person number 651900002");
        assertThat(entry.getNewValues().get(2).getColumnName()).isEqualTo("AGE");
        assertThat(entry.getNewValues().get(2).getColumnData()).isEqualTo("125");
        assertThat(entry.getNewValues().get(3).getColumnName()).isEqualTo("ADRESS");
        assertThat(entry.getNewValues().get(3).getColumnData()).isEqualTo("street:651900002 av: 651900002 house: 651900002");
        assertThat(entry.getNewValues().get(4).getColumnName()).isEqualTo("TD");
        assertThat(entry.getNewValues().get(4).getColumnData()).isEqualTo("TO_DATE('15-MAY-21', 'DD-MON-RR')");
        assertThat(entry.getNewValues().get(5).getColumnName()).isEqualTo("FLAG");
        assertThat(entry.getNewValues().get(5).getColumnData()).isNull();
    }

}
