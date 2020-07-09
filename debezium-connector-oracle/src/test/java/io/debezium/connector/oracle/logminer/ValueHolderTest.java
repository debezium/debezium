/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.jsqlparser.SimpleDmlParser;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValue;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValueImpl;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValueWrapper;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntryImpl;
import io.debezium.data.Envelope;
import io.debezium.relational.Tables;
import io.debezium.util.IoUtil;

@SkipWhenAdapterNameIsNot(value = AdapterName.LOGMINER)
public class ValueHolderTest {
    private static final String TABLE_NAME = "TEST";
    private static final String CATALOG_NAME = "CATALOG";
    private static final String SCHEMA_NAME = "DEBEZIUM";
    private OracleDdlParser ddlParser;
    private SimpleDmlParser sqlDmlParser;
    private Tables tables;
    private static final String FULL_TABLE_NAME = SCHEMA_NAME + "\".\"" + TABLE_NAME;

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    @Before
    public void setUp() {
        OracleChangeRecordValueConverter converters = new OracleChangeRecordValueConverter(null);
        ddlParser = new OracleDdlParser(true, CATALOG_NAME, SCHEMA_NAME);
        sqlDmlParser = new SimpleDmlParser(CATALOG_NAME, SCHEMA_NAME, converters);
        tables = new Tables();
    }

    @Test
    public void testValueHolders() throws Exception {
        LogMinerColumnValue column1 = new LogMinerColumnValueImpl("COLUMN1", Types.NUMERIC);
        assertThat(column1.equals(column1)).isTrue();
        assertThat(column1.equals(null)).isFalse();
        assertThat(new LogMinerColumnValueWrapper(column1).isProcessed()).isFalse();

        column1.setColumnData(new BigDecimal(5));
        LogMinerColumnValue column2 = new LogMinerColumnValueImpl("COLUMN2", Types.VARCHAR);
        column2.setColumnData("Text");
        List<LogMinerColumnValue> newValues = new ArrayList<>();
        newValues.add(column1);
        newValues.add(column2);
        LogMinerDmlEntryImpl dmlEntryExpected = new LogMinerDmlEntryImpl(Envelope.Operation.CREATE, newValues, Collections.emptyList());
        dmlEntryExpected.setTransactionId("transaction_id");
        dmlEntryExpected.setObjectName(TABLE_NAME);
        dmlEntryExpected.setObjectOwner(SCHEMA_NAME);
        dmlEntryExpected.setScn(BigDecimal.ONE);
        dmlEntryExpected.setSourceTime(new Timestamp(1000));

        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_small_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);

        String dml = "insert into \"" + FULL_TABLE_NAME + "\"  (\"column1\",\"column2\") values ('5','Text');";
        LogMinerDmlEntry dmlEntryParsed = sqlDmlParser.parse(dml, tables, "1");

        assertThat(dmlEntryParsed.equals(dmlEntryExpected)).isTrue();
        assertThat(dmlEntryExpected.getCommandType() == Envelope.Operation.CREATE).isTrue();
        assertThat(dmlEntryExpected.getScn().equals(BigDecimal.ONE)).isTrue();
        assertThat(dmlEntryExpected.getSourceTime().equals(new Timestamp(1000))).isTrue();
        assertThat(dmlEntryExpected.getTransactionId().equals("transaction_id")).isTrue();
        assertThat(dmlEntryExpected.getObjectOwner().equals(SCHEMA_NAME)).isTrue();
        assertThat(dmlEntryExpected.getObjectName().equals(TABLE_NAME)).isTrue();

        assertThat(dmlEntryExpected.equals(null)).isFalse();
        assertThat(dmlEntryExpected.equals(dmlEntryExpected)).isTrue();
    }
}
