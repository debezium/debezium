/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import oracle.sql.CHAR;
import oracle.sql.NUMBER;
import oracle.streams.ColumnValue;
import oracle.streams.DefaultColumnValue;
import oracle.streams.DefaultRowLCR;
import oracle.streams.RowLCR;

/**
 * Unit tests for the LCR column validation done by {@link LcrEventHandler} before a change record emitter is built.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(SkipWhenAdapterNameIsNot.AdapterName.XSTREAM)
public class LcrEventHandlerTest {

    private static final TableId TABLE_ID = new TableId("ORCLPDB1", "DEBEZIUM", "DBZ1867");

    private Table table;

    @BeforeEach
    void before() {
        table = Table.editor()
                .tableId(TABLE_ID)
                .addColumn(Column.editor().name("ID").jdbcType(Types.NUMERIC).type("NUMBER").position(1).create())
                .addColumn(Column.editor().name("DATA").jdbcType(Types.VARCHAR).type("VARCHAR2").position(2).create())
                .setPrimaryKeyNames("ID")
                .create();
    }

    @Test
    @FixFor("debezium/dbz#1867")
    void shouldFindNoUnknownColumnsWhenAllColumnsAreKnown() throws Exception {
        final RowLCR lcr = createLcr(RowLCR.UPDATE);
        lcr.setOldValues(new ColumnValue[]{ numberValue("ID", 1), charValue("data", "abc") });
        lcr.setNewValues(new ColumnValue[]{ numberValue("ID", 1), charValue(OracleDatabaseSchema.ROW_ARCHIVAL_COLUMN_NAME, "0") });

        final Map<String, Object> chunkValues = new HashMap<>();
        chunkValues.put("DATA", "chunk data");

        assertThat(LcrEventHandler.getColumnsNotInSchema(lcr, table, chunkValues)).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#1867")
    void shouldFindUnknownColumnsAcrossOldNewAndChunkValues() throws Exception {
        final RowLCR lcr = createLcr(RowLCR.UPDATE);
        lcr.setOldValues(new ColumnValue[]{ numberValue("ID", 1), charValue("OLD_ONLY", "a") });
        lcr.setNewValues(new ColumnValue[]{ numberValue("ID", 1), charValue("NEW_ONLY", "b"), charValue("OLD_ONLY", "c") });

        final Map<String, Object> chunkValues = new HashMap<>();
        chunkValues.put("DATA", "chunk data");
        chunkValues.put("CHUNK_ONLY", "chunk data");

        assertThat(LcrEventHandler.getColumnsNotInSchema(lcr, table, chunkValues))
                .containsExactly("OLD_ONLY", "NEW_ONLY", "CHUNK_ONLY");
    }

    @Test
    @FixFor("debezium/dbz#1867")
    void shouldFindNoUnknownColumnsWhenLcrHasNoValues() {
        final RowLCR lcr = createLcr("TRUNCATE TABLE");
        assertThat(LcrEventHandler.getColumnsNotInSchema(lcr, table, new HashMap<>())).isEmpty();
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
