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

/**
 * @author Chris Cranford
 */
public class LogMinerDmlParserTest {

    private static final String CATALOG_NAME = "ORCLCDB1";
    private static final String SCHEMA_NAME = "DEBEZIUM";

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
        String sql = "insert into \"DEBEZIUM\".\"TEST\"(\"ID\",\"NAME\",\"TS\",\"UT\",\"DATE\",\"UT2\",\"C1\",\"C2\") values " +
                "('1','Acme',TO_TIMESTAMP('2020-02-01 00:00:00.'),Unsupported Type," +
                "TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),Unsupported Type,NULL,NULL);";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, null, null, null);
        assertThat(entry.getCommandType()).isEqualTo(Operation.CREATE);
        assertThat(entry.getOldValues()).isEmpty();
        assertThat(entry.getNewValues()).hasSize(8);
        assertThat(entry.getNewValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getNewValues().get(1).getColumnName()).isEqualTo("NAME");
        assertThat(entry.getNewValues().get(2).getColumnName()).isEqualTo("TS");
        assertThat(entry.getNewValues().get(3).getColumnName()).isEqualTo("UT");
        assertThat(entry.getNewValues().get(4).getColumnName()).isEqualTo("DATE");
        assertThat(entry.getNewValues().get(5).getColumnName()).isEqualTo("UT2");
        assertThat(entry.getNewValues().get(6).getColumnName()).isEqualTo("C1");
        assertThat(entry.getNewValues().get(0).getColumnData()).isEqualTo("1");
        assertThat(entry.getNewValues().get(1).getColumnData()).isEqualTo("Acme");
        assertThat(entry.getNewValues().get(2).getColumnData()).isEqualTo("TO_TIMESTAMP('2020-02-01 00:00:00.')");
        assertThat(entry.getNewValues().get(3).getColumnData()).isNull();
        assertThat(entry.getNewValues().get(4).getColumnData()).isEqualTo("TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')");
        assertThat(entry.getNewValues().get(5).getColumnData()).isNull();
        assertThat(entry.getNewValues().get(6).getColumnData()).isNull();
        assertThat(entry.getNewValues().get(7).getColumnData()).isNull();
    }

    @Test
    @FixFor("DBZ-3078")
    public void testParsingUpdate() throws Exception {
        String sql = "update \"DEBEZIUM\".\"TEST\" " +
                "set \"NAME\" = 'Bob', \"TS\" = TO_TIMESTAMP('2020-02-02 00:00:00.'), \"UT\" = Unsupported Type, " +
                "\"DATE\" = TO_DATE('2020-02-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), \"UT2\" = Unsupported Type, " +
                "\"C1\" = NULL where \"ID\" = '1' and \"NAME\" = 'Acme' and \"TS\" = TO_TIMESTAMP('2020-02-01 00:00:00.') and " +
                "\"UT\" = Unsupported Type and \"DATE\" = TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') and " +
                "\"UT2\" = Unsupported Type and \"C1\" = NULL and \"IS\" IS NULL and \"IS2\" IS NULL;";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, null, null, null);
        assertThat(entry.getCommandType()).isEqualTo(Operation.UPDATE);
        assertThat(entry.getOldValues()).hasSize(9);
        assertThat(entry.getOldValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getOldValues().get(1).getColumnName()).isEqualTo("NAME");
        assertThat(entry.getOldValues().get(2).getColumnName()).isEqualTo("TS");
        assertThat(entry.getOldValues().get(3).getColumnName()).isEqualTo("UT");
        assertThat(entry.getOldValues().get(4).getColumnName()).isEqualTo("DATE");
        assertThat(entry.getOldValues().get(5).getColumnName()).isEqualTo("UT2");
        assertThat(entry.getOldValues().get(0).getColumnData()).isEqualTo("1");
        assertThat(entry.getOldValues().get(1).getColumnData()).isEqualTo("Acme");
        assertThat(entry.getOldValues().get(2).getColumnData()).isEqualTo("TO_TIMESTAMP('2020-02-01 00:00:00.')");
        assertThat(entry.getOldValues().get(3).getColumnData()).isNull();
        assertThat(entry.getOldValues().get(4).getColumnData()).isEqualTo("TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')");
        assertThat(entry.getOldValues().get(5).getColumnData()).isNull();
        assertThat(entry.getOldValues().get(6).getColumnData()).isNull();
        assertThat(entry.getOldValues().get(7).getColumnData()).isNull();
        assertThat(entry.getNewValues()).hasSize(9);
        assertThat(entry.getNewValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getNewValues().get(1).getColumnName()).isEqualTo("NAME");
        assertThat(entry.getNewValues().get(2).getColumnName()).isEqualTo("TS");
        assertThat(entry.getNewValues().get(3).getColumnName()).isEqualTo("UT");
        assertThat(entry.getNewValues().get(4).getColumnName()).isEqualTo("DATE");
        assertThat(entry.getNewValues().get(5).getColumnName()).isEqualTo("UT2");
        assertThat(entry.getNewValues().get(0).getColumnData()).isEqualTo("1");
        assertThat(entry.getNewValues().get(1).getColumnData()).isEqualTo("Bob");
        assertThat(entry.getNewValues().get(2).getColumnData()).isEqualTo("TO_TIMESTAMP('2020-02-02 00:00:00.')");
        assertThat(entry.getNewValues().get(3).getColumnData()).isNull();
        assertThat(entry.getNewValues().get(4).getColumnData()).isEqualTo("TO_DATE('2020-02-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')");
        assertThat(entry.getNewValues().get(5).getColumnData()).isNull();
        assertThat(entry.getNewValues().get(6).getColumnData()).isNull();
        assertThat(entry.getNewValues().get(7).getColumnData()).isNull();
        assertThat(entry.getNewValues().get(8).getColumnData()).isNull();
    }

    @Test
    @FixFor("DBZ-3078")
    public void testParsingDelete() throws Exception {
        String sql = "delete from \"DEBEZIUM\".\"TEST\" " +
                "where \"ID\" = '1' and \"NAME\" = 'Acme' and \"TS\" = TO_TIMESTAMP('2020-02-01 00:00:00.') and " +
                "\"UT\" = Unsupported Type and \"DATE\" = TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') and " +
                "\"IS\" IS NULL and \"IS2\" IS NULL;";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, null, null, null);
        assertThat(entry.getCommandType()).isEqualTo(Operation.DELETE);
        assertThat(entry.getOldValues()).hasSize(7);
        assertThat(entry.getOldValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getOldValues().get(1).getColumnName()).isEqualTo("NAME");
        assertThat(entry.getOldValues().get(2).getColumnName()).isEqualTo("TS");
        assertThat(entry.getOldValues().get(3).getColumnName()).isEqualTo("UT");
        assertThat(entry.getOldValues().get(4).getColumnName()).isEqualTo("DATE");
        assertThat(entry.getOldValues().get(0).getColumnData()).isEqualTo("1");
        assertThat(entry.getOldValues().get(1).getColumnData()).isEqualTo("Acme");
        assertThat(entry.getOldValues().get(2).getColumnData()).isEqualTo("TO_TIMESTAMP('2020-02-01 00:00:00.')");
        assertThat(entry.getOldValues().get(3).getColumnData()).isNull();
        assertThat(entry.getOldValues().get(4).getColumnData()).isEqualTo("TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')");
        assertThat(entry.getOldValues().get(5).getColumnData()).isNull();
        assertThat(entry.getOldValues().get(6).getColumnData()).isNull();
        assertThat(entry.getNewValues()).isEmpty();
    }
}
