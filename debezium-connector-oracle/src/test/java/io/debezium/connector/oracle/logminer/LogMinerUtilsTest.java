/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName;

@SkipWhenAdapterNameIsNot(value = AdapterName.LOGMINER)
public class LogMinerUtilsTest {

    private static final Scn SCN = new Scn(BigInteger.ONE);
    private static final Scn OTHER_SCN = new Scn(BigInteger.TEN);

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    @Test
    public void testStartLogMinerStatement() {
        String statement = SqlUtils.startLogMinerStatement(SCN, OTHER_SCN, OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO, false, null);
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_REDO_LOGS")).isTrue();
        assertThat(statement.contains("DBMS_LOGMNR.DDL_DICT_TRACKING")).isTrue();
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG")).isFalse();
        assertThat(statement.contains("DBMS_LOGMNR.CONTINUOUS_MINE")).isFalse();
        statement = SqlUtils.startLogMinerStatement(SCN, OTHER_SCN, OracleConnectorConfig.LogMiningStrategy.ONLINE_CATALOG, false, null);
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_REDO_LOGS")).isFalse();
        assertThat(statement.contains("DBMS_LOGMNR.DDL_DICT_TRACKING")).isFalse();
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG")).isTrue();
        assertThat(statement.contains("DBMS_LOGMNR.CONTINUOUS_MINE")).isFalse();
        statement = SqlUtils.startLogMinerStatement(SCN, OTHER_SCN, OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO, true, null);
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_REDO_LOGS")).isTrue();
        assertThat(statement.contains("DBMS_LOGMNR.DDL_DICT_TRACKING")).isTrue();
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG")).isFalse();
        assertThat(statement.contains("DBMS_LOGMNR.CONTINUOUS_MINE")).isTrue();
        statement = SqlUtils.startLogMinerStatement(SCN, OTHER_SCN, OracleConnectorConfig.LogMiningStrategy.ONLINE_CATALOG, true, null);
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_REDO_LOGS")).isFalse();
        assertThat(statement.contains("DBMS_LOGMNR.DDL_DICT_TRACKING")).isFalse();
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG")).isTrue();
        assertThat(statement.contains("DBMS_LOGMNR.CONTINUOUS_MINE")).isTrue();
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
