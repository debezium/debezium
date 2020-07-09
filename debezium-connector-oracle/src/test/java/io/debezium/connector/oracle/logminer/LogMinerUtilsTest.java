/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleSnapshotChangeEventSource;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.IoUtil;

@SkipWhenAdapterNameIsNot(value = AdapterName.LOGMINER)
public class LogMinerUtilsTest {

    private static final BigDecimal SCN = BigDecimal.ONE;
    private static final BigDecimal OTHER_SCN = BigDecimal.TEN;
    private OracleDdlParser ddlParser;
    private Tables tables;
    private static final String TABLE_NAME = "TEST";
    private static final String CATALOG_NAME = "ORCLPDB1";
    private static final String SCHEMA_NAME = "DEBEZIUM";

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    @Test
    public void testStartLogMinerStatement() {
        String statement = SqlUtils.getStartLogMinerStatement(SCN.longValue(), OTHER_SCN.longValue(), OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO, false);
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_REDO_LOGS")).isTrue();
        assertThat(statement.contains("DBMS_LOGMNR.DDL_DICT_TRACKING")).isTrue();
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG")).isFalse();
        assertThat(statement.contains("DBMS_LOGMNR.CONTINUOUS_MINE")).isFalse();
        statement = SqlUtils.getStartLogMinerStatement(SCN.longValue(), OTHER_SCN.longValue(), OracleConnectorConfig.LogMiningStrategy.ONLINE_CATALOG, false);
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_REDO_LOGS")).isFalse();
        assertThat(statement.contains("DBMS_LOGMNR.DDL_DICT_TRACKING")).isFalse();
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG")).isTrue();
        assertThat(statement.contains("DBMS_LOGMNR.CONTINUOUS_MINE")).isFalse();
        statement = SqlUtils.getStartLogMinerStatement(SCN.longValue(), OTHER_SCN.longValue(), OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO, true);
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_REDO_LOGS")).isTrue();
        assertThat(statement.contains("DBMS_LOGMNR.DDL_DICT_TRACKING")).isTrue();
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG")).isFalse();
        assertThat(statement.contains("DBMS_LOGMNR.CONTINUOUS_MINE")).isTrue();
        statement = SqlUtils.getStartLogMinerStatement(SCN.longValue(), OTHER_SCN.longValue(), OracleConnectorConfig.LogMiningStrategy.ONLINE_CATALOG, true);
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_REDO_LOGS")).isFalse();
        assertThat(statement.contains("DBMS_LOGMNR.DDL_DICT_TRACKING")).isFalse();
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG")).isTrue();
        assertThat(statement.contains("DBMS_LOGMNR.CONTINUOUS_MINE")).isTrue();
    }

    @Test
    public void testBlacklistFiltering() throws Exception {

        ddlParser = new OracleDdlParser(true, CATALOG_NAME, SCHEMA_NAME);
        tables = new Tables();
        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);
        Table table = tables.forTable(new TableId(CATALOG_NAME, SCHEMA_NAME, TABLE_NAME));

        String prefix = CATALOG_NAME + "." + TABLE_NAME + ".";
        String blacklistedColumns = prefix + "COL2," + prefix + "COL3";
        String whitelistedColumns = OracleSnapshotChangeEventSource.buildSelectColumns(blacklistedColumns, table);
        assertThat(whitelistedColumns.contains("COL2")).isFalse();
        assertThat(whitelistedColumns.contains("COL3")).isFalse();
        assertThat(whitelistedColumns.contains("COL4")).isTrue();

        prefix = TABLE_NAME + ".";
        blacklistedColumns = prefix + "COL2," + prefix + "COL3";
        whitelistedColumns = OracleSnapshotChangeEventSource.buildSelectColumns(blacklistedColumns.toLowerCase(), table);
        assertThat(whitelistedColumns.contains("COL2")).isFalse();
        assertThat(whitelistedColumns.contains("COL3")).isFalse();
        assertThat(whitelistedColumns.contains("COL4")).isTrue();

        prefix = "";
        blacklistedColumns = prefix + "COL2," + prefix + "COL3";
        whitelistedColumns = OracleSnapshotChangeEventSource.buildSelectColumns(blacklistedColumns, table);
        assertThat(whitelistedColumns.equals("*")).isTrue();

        prefix = "NONEXISTINGTABLE.";
        blacklistedColumns = prefix + "COL2," + prefix + "COL3";
        whitelistedColumns = OracleSnapshotChangeEventSource.buildSelectColumns(blacklistedColumns, table);
        assertThat(whitelistedColumns.equals("*")).isTrue();

        prefix = TABLE_NAME + ".";
        blacklistedColumns = prefix + "col2," + prefix + "CO77";
        whitelistedColumns = OracleSnapshotChangeEventSource.buildSelectColumns(blacklistedColumns, table);
        assertThat(whitelistedColumns.contains("COL2")).isFalse();
        assertThat(whitelistedColumns.contains("CO77")).isFalse();
        assertThat(whitelistedColumns.contains("COL4")).isTrue();

        blacklistedColumns = "";
        whitelistedColumns = OracleSnapshotChangeEventSource.buildSelectColumns(blacklistedColumns, table);
        assertThat(whitelistedColumns.equals("*")).isTrue();

        blacklistedColumns = null;
        whitelistedColumns = OracleSnapshotChangeEventSource.buildSelectColumns(blacklistedColumns, table);
        assertThat(whitelistedColumns.equals("*")).isTrue();
    }

    // todo delete after replacement == -1 in the code
    @Test
    public void testConversion() {
        Map<String, String> map = new HashMap<>();
        map.put("one", "1001");
        map.put("two", "1002");
        map.put("three", "1007");
        map.put("four", "18446744073709551615");
        Map<String, Long> res = map.entrySet().stream()
                .filter(entry -> new BigDecimal(entry.getValue()).longValue() > 1003 || new BigDecimal(entry.getValue()).longValue() == -1).collect(Collectors
                        .toMap(Map.Entry::getKey, e -> new BigDecimal(e.getValue()).longValue() == -1 ? Long.MAX_VALUE : new BigInteger(e.getValue()).longValue()));

        assertThat(res).isNotEmpty();
    }
}
