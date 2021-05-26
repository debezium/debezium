/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName;
import io.debezium.connector.oracle.logminer.parser.SimpleDmlParser;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValue;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValueImpl;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValueWrapper;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntryImpl;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.IoUtil;

@SkipWhenAdapterNameIsNot(value = AdapterName.LOGMINER)
public class ValueHolderTest {
    private static final Scn SCN_ONE = new Scn(BigInteger.ONE);
    private static final String TABLE_NAME = "TEST";
    private static final String CATALOG_NAME = "CATALOG";
    private static final String SCHEMA_NAME = "DEBEZIUM";
    private OracleDdlParser ddlParser;
    private SimpleDmlParser sqlDmlParser;
    private Tables tables;
    private static final String FULL_TABLE_NAME = SCHEMA_NAME + "\".\"" + TABLE_NAME;
    private static final TableId TABLE_ID = TableId.parse(CATALOG_NAME + "." + SCHEMA_NAME + "." + TABLE_NAME);

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
    }

    @Test
    public void testValueHolders() throws Exception {
        LogMinerColumnValue column1 = new LogMinerColumnValueImpl("COLUMN1");
        assertThat(column1.equals(column1)).isTrue();
        assertThat(column1.equals(null)).isFalse();
        assertThat(new LogMinerColumnValueWrapper(column1).isProcessed()).isFalse();

        column1.setColumnData(new BigDecimal(5));
        LogMinerColumnValue column2 = new LogMinerColumnValueImpl("COLUMN2");
        column2.setColumnData("Text");
        List<LogMinerColumnValue> newValues = new ArrayList<>();
        newValues.add(column1);
        newValues.add(column2);
        LogMinerDmlEntry dmlEntryExpected = LogMinerDmlEntryImpl.forInsert(newValues);
        dmlEntryExpected.setObjectName(TABLE_NAME);
        dmlEntryExpected.setObjectOwner(SCHEMA_NAME);

        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_small_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);

        String dml = "insert into \"" + FULL_TABLE_NAME + "\"  (\"column1\",\"column2\") values ('5','Text');";
        LogMinerDmlEntry dmlEntryParsed = sqlDmlParser.parse(dml, tables.forTable(TABLE_ID), "1");

        assertThat(dmlEntryParsed.equals(dmlEntryExpected)).isTrue();
        assertThat(dmlEntryExpected.getOperation()).isEqualTo(RowMapper.INSERT);
        assertThat(dmlEntryExpected.getObjectOwner().equals(SCHEMA_NAME)).isTrue();
        assertThat(dmlEntryExpected.getObjectName().equals(TABLE_NAME)).isTrue();

        assertThat(dmlEntryExpected.equals(null)).isFalse();
        assertThat(dmlEntryExpected.equals(dmlEntryExpected)).isTrue();
    }
}
