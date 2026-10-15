/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.relational;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.JsonTableChangeSerializer;

/**
 * Verifies that the Oracle connector creates, edits and recovers its tables and columns as
 * {@link OracleTable} and {@link OracleColumn} instances.
 *
 * @author Chris Cranford
 */
public class OracleRelationalMetadataFactoryTest {

    private static final TableId TABLE_ID = new TableId("ORCLPDB1", "DEBEZIUM", "TEST");

    private OracleDdlParser parser;
    private Tables tables;

    @BeforeEach
    void beforeEach() {
        parser = new OracleDdlParser();
        parser.setCurrentDatabase(TABLE_ID.catalog());
        parser.setCurrentSchema(TABLE_ID.schema());
        tables = new Tables(false, OracleRelationalMetadataFactory.INSTANCE);
    }

    @Test
    @FixFor("debezium/dbz#2843")
    void shouldUseOracleFactoryFromConnectorConfig() {
        final OracleConnectorConfig config = new OracleConnectorConfig(TestHelper.defaultConfig().build());
        assertThat(config.getRelationalMetadataFactory()).isSameAs(OracleRelationalMetadataFactory.INSTANCE);
    }

    @Test
    @FixFor("debezium/dbz#2843")
    void shouldCreateOracleTypesWhenParsingCreateAndAlterTable() {
        parser.parse("CREATE TABLE TEST (ID NUMBER(9) PRIMARY KEY, DATA VARCHAR2(50))", tables);
        assertOracleTypes(tables.forTable(TABLE_ID), "ID", "DATA");

        parser.parse("ALTER TABLE TEST ADD (DATA2 NUMBER(5), DATA3 DATE)", tables);
        assertOracleTypes(tables.forTable(TABLE_ID), "ID", "DATA", "DATA2", "DATA3");

        parser.parse("ALTER TABLE TEST RENAME COLUMN DATA TO DATA1", tables);
        assertOracleTypes(tables.forTable(TABLE_ID), "ID", "DATA1", "DATA2", "DATA3");
    }

    @Test
    @FixFor("debezium/dbz#2843")
    void shouldKeepOracleTypesWhenEditing() {
        parser.parse("CREATE TABLE TEST (ID NUMBER(9) PRIMARY KEY, DATA VARCHAR2(50))", tables);
        final Table table = tables.forTable(TABLE_ID);

        assertThat(table.edit().create()).isInstanceOf(OracleTable.class).isEqualTo(table);
        assertThat(table.columnWithName("DATA").edit().create()).isInstanceOf(OracleColumn.class);
    }

    @Test
    @FixFor("debezium/dbz#2843")
    void shouldRecoverOracleTypesFromSchemaHistoryDocument() {
        parser.parse("CREATE TABLE TEST (ID NUMBER(9) PRIMARY KEY, DATA VARCHAR2(50))", tables);
        final Table table = tables.forTable(TABLE_ID);

        final JsonTableChangeSerializer serializer = OracleRelationalMetadataFactory.INSTANCE.jsonTableChangesSerializer();
        final Table recovered = serializer.fromDocument(TABLE_ID, serializer.toDocument(table));

        assertOracleTypes(recovered, "ID", "DATA");
        assertThat(recovered).isEqualTo(table);
    }

    @Test
    @FixFor("debezium/dbz#2843")
    void shouldNotEqualBaseTypesWithSameState() {
        final Column column = dataColumn(OracleRelationalMetadataFactory.INSTANCE.columnEditor());
        assertThat(column).isNotEqualTo(dataColumn(Column.editor()));

        final Table table = OracleRelationalMetadataFactory.INSTANCE.tableEditor().tableId(TABLE_ID).addColumn(column).create();
        assertThat(table).isNotEqualTo(Table.editor().tableId(TABLE_ID).addColumn(column).create());
    }

    private static Column dataColumn(ColumnEditor editor) {
        return editor.name("DATA").type("VARCHAR2").jdbcType(Types.VARCHAR).length(50).create();
    }

    private static void assertOracleTypes(Table table, String... columnNames) {
        assertThat(table).isInstanceOf(OracleTable.class);
        assertThat(table.retrieveColumnNames()).containsExactly(columnNames);
        assertThat(table.columns()).allMatch(OracleColumn.class::isInstance);
    }
}
