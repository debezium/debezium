/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope.Operation;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

import oracle.sql.CHAR;
import oracle.sql.NUMBER;
import oracle.streams.ColumnValue;
import oracle.streams.DefaultColumnValue;
import oracle.streams.DefaultRowLCR;
import oracle.streams.RowLCR;

/**
 * Unit tests for {@link XStreamChangeRecordEmitter}.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(SkipWhenAdapterNameIsNot.AdapterName.XSTREAM)
public class XStreamChangeRecordEmitterTest {

    private static final TableId TABLE_ID = new TableId("ORCLPDB1", "DEBEZIUM", "DBZ1867");

    private OracleConnectorConfig connectorConfig;
    private OraclePartition partition;
    private Table table;

    @BeforeEach
    void before() {
        connectorConfig = new OracleConnectorConfig(TestHelper.defaultConfig().build());
        partition = new OraclePartition(connectorConfig.getLogicalName(), TABLE_ID.catalog());
        table = Table.editor()
                .tableId(TABLE_ID)
                .addColumn(Column.editor().name("ID").jdbcType(Types.NUMERIC).type("NUMBER").position(1).create())
                .addColumn(Column.editor().name("DATA").jdbcType(Types.VARCHAR).type("VARCHAR2").position(2).create())
                .setPrimaryKeyNames("ID")
                .create();
    }

    @Test
    @FixFor("debezium/dbz#1867")
    void shouldBuildValuesWhenAllColumnsAreKnown() throws Exception {
        final RowLCR lcr = createLcr(RowLCR.INSERT);
        lcr.setNewValues(new ColumnValue[]{ numberValue("ID", 1), charValue("DATA", "abc") });

        final XStreamChangeRecordEmitter emitter = createEmitter(lcr, new HashMap<>(), new HashMap<>());
        assertThat(emitter.getOperation()).isEqualTo(Operation.CREATE);
    }

    @Test
    @FixFor("debezium/dbz#1867")
    void shouldIgnoreRowArchivalColumn() throws Exception {
        final RowLCR lcr = createLcr(RowLCR.INSERT);
        lcr.setNewValues(new ColumnValue[]{ numberValue("ID", 1), charValue(OracleDatabaseSchema.ROW_ARCHIVAL_COLUMN_NAME, "0") });

        final XStreamChangeRecordEmitter emitter = createEmitter(lcr, new HashMap<>(), new HashMap<>());
        assertThat(emitter.getOperation()).isEqualTo(Operation.CREATE);
    }

    @Test
    @FixFor("debezium/dbz#1867")
    void shouldFailWithDescriptiveErrorWhenNewValueColumnIsNotInSchema() throws Exception {
        final RowLCR lcr = createLcr(RowLCR.INSERT);
        lcr.setNewValues(new ColumnValue[]{ numberValue("ID", 1), charValue("UNKNOWN_COL", "abc") });

        assertThatThrownBy(() -> createEmitter(lcr, new HashMap<>(), new HashMap<>()))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("UNKNOWN_COL")
                .hasMessageContaining(TABLE_ID.toString());
    }

    @Test
    @FixFor("debezium/dbz#1867")
    void shouldFailWithDescriptiveErrorWhenOldValueColumnIsNotInSchema() throws Exception {
        final RowLCR lcr = createLcr(RowLCR.DELETE);
        lcr.setOldValues(new ColumnValue[]{ numberValue("ID", 1), charValue("UNKNOWN_COL", "abc") });

        assertThatThrownBy(() -> createEmitter(lcr, new HashMap<>(), new HashMap<>()))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("UNKNOWN_COL")
                .hasMessageContaining(TABLE_ID.toString());
    }

    @Test
    @FixFor("debezium/dbz#1867")
    void shouldFailWithDescriptiveErrorWhenChunkColumnIsNotInSchema() throws Exception {
        final RowLCR lcr = createLcr(RowLCR.UPDATE);
        lcr.setOldValues(new ColumnValue[]{ numberValue("ID", 1) });
        lcr.setNewValues(new ColumnValue[]{ numberValue("ID", 1) });

        final Map<String, Object> chunkValues = new HashMap<>();
        chunkValues.put("UNKNOWN_CLOB", "chunk data");

        assertThatThrownBy(() -> createEmitter(lcr, new HashMap<>(), chunkValues))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("UNKNOWN_CLOB")
                .hasMessageContaining(TABLE_ID.toString());
    }

    @Test
    @FixFor("debezium/dbz#1867")
    void shouldFailWithDescriptiveErrorWhenLobOperationChunkColumnIsNotInSchema() throws Exception {
        final RowLCR lcr = createLcr(RowLCR.LOB_WRITE);
        lcr.setOldValues(new ColumnValue[]{ numberValue("ID", 1) });

        final Map<String, Object> oldChunkValues = new HashMap<>();
        oldChunkValues.put("UNKNOWN_CLOB", "old chunk data");

        assertThatThrownBy(() -> createEmitter(lcr, oldChunkValues, new HashMap<>()))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("UNKNOWN_CLOB")
                .hasMessageContaining(TABLE_ID.toString());
    }

    private XStreamChangeRecordEmitter createEmitter(RowLCR lcr, Map<String, Object> oldChunkValues, Map<String, Object> newChunkValues) {
        return new XStreamChangeRecordEmitter(connectorConfig, partition, null, lcr, oldChunkValues, newChunkValues,
                table, null, Clock.system());
    }

    private static RowLCR createLcr(String commandType) {
        return new DefaultRowLCR(TABLE_ID.catalog(), commandType, TABLE_ID.schema(), TABLE_ID.table(),
                "1.2.3", null, new byte[]{ 1 }, null);
    }

    private static ColumnValue numberValue(String columnName, int value) throws Exception {
        return new DefaultColumnValue(columnName, new NUMBER(value));
    }

    private static ColumnValue charValue(String columnName, String value) throws Exception {
        return new DefaultColumnValue(columnName, new CHAR(value, null));
    }
}
