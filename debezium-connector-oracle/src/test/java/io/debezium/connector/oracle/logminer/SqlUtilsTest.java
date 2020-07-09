/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.HashSet;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName;
import io.debezium.relational.TableId;

@SkipWhenAdapterNameIsNot(value = AdapterName.LOGMINER)
public class SqlUtilsTest {

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    @Test
    public void testStatements() {
        String result = SqlUtils.getAddLogFileStatement("ADD", "FILENAME");
        String expected = "BEGIN sys.dbms_logmnr.add_logfile(LOGFILENAME => 'FILENAME', OPTIONS => ADD);END;";
        assertThat(expected.equals(result)).isTrue();

        OracleDatabaseSchema schema = mock(OracleDatabaseSchema.class);
        TableId table1 = new TableId("catalog", "schema", "table1");
        TableId table2 = new TableId("catalog", "schema", "table2");
        Set<TableId> tables = new HashSet<>();
        Mockito.when(schema.tableIds()).thenReturn(tables);
        result = SqlUtils.queryLogMinerContents("DATABASE", "SCHEMA", schema);
        expected = "SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME  " +
                "FROM V$LOGMNR_CONTENTS WHERE  OPERATION_CODE in (1,2,3,5)  AND SEG_OWNER = 'DATABASE'  AND SCN >= ? AND SCN < ?  " +
                "OR (OPERATION_CODE IN (5,7,34,36) AND USERNAME NOT IN ('SYS','SYSTEM','SCHEMA'))ORDER BY SCN";
        assertThat(expected.equals(result)).isTrue();

        tables.add(table1);
        tables.add(table2);
        result = SqlUtils.queryLogMinerContents("DATABASE", "SCHEMA", schema);
        expected = "SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME  " +
                "FROM V$LOGMNR_CONTENTS WHERE  OPERATION_CODE in (1,2,3,5)  " +
                "AND SEG_OWNER = 'DATABASE'  AND table_name IN ('table1','table2') AND SEG_NAME IN ('table1','table2')  " +
                "AND SCN >= ? AND SCN < ?  OR (OPERATION_CODE IN (5,7,34,36) AND USERNAME NOT IN ('SYS','SYSTEM','SCHEMA'))ORDER BY SCN";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.getStartLogMinerStatement(10L, 20L, OracleConnectorConfig.LogMiningStrategy.ONLINE_CATALOG, true);
        expected = "BEGIN sys.dbms_logmnr.start_logmnr(startScn => '10', endScn => '20', " +
                "OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG  + DBMS_LOGMNR.CONTINUOUS_MINE  + DBMS_LOGMNR.NO_ROWID_IN_STMT);END;";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.getStartLogMinerStatement(10L, 20L, OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO, false);
        expected = "BEGIN sys.dbms_logmnr.start_logmnr(startScn => '10', endScn => '20', " +
                "OPTIONS => DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING  + DBMS_LOGMNR.NO_ROWID_IN_STMT);END;";
        assertThat(expected.equals(result)).isTrue();

    }
}
