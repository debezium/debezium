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
import io.debezium.document.Document;
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
        final Column plainColumn = dataColumn(Column.editor());
        assertThat(column).isNotEqualTo(plainColumn);
        assertThat(plainColumn).isNotEqualTo(column);

        final Table table = OracleRelationalMetadataFactory.INSTANCE.tableEditor().tableId(TABLE_ID).addColumn(column).create();
        final Table plainTable = Table.editor().tableId(TABLE_ID).addColumn(column).create();
        assertThat(table).isNotEqualTo(plainTable);
        assertThat(plainTable).isNotEqualTo(table);
    }

    @Test
    @FixFor("debezium/dbz#2794")
    void shouldKeepOctetLengthWhenCreatingAndEditing() {
        final OracleColumn column = varcharColumn(8000);
        assertThat(column.octetLength()).isEqualTo(8000);
        assertThat(column.edit().octetLength()).isEqualTo(8000);
        assertThat(column.edit().create()).isEqualTo(column);
        assertThat(column.toString()).contains("OCTET_LENGTH 8000");

        final OracleColumn unset = varcharColumn(Column.UNSET_INT_VALUE);
        assertThat(unset.octetLength()).isEqualTo(Column.UNSET_INT_VALUE);
        assertThat(unset.toString()).doesNotContain("OCTET_LENGTH");
    }

    @Test
    @FixFor("debezium/dbz#2794")
    void shouldIncludeOctetLengthInEquality() {
        final OracleColumn column = varcharColumn(8000);
        final OracleColumn other = column.edit().octetLength(2000).create();
        assertThat(other).isNotEqualTo(column);
        assertThat(column).isNotEqualTo(other);
        assertThat(column.edit().create().hashCode()).isEqualTo(column.hashCode());
    }

    @Test
    @FixFor("debezium/dbz#2794")
    void shouldRoundTripOctetLengthThroughSchemaHistoryDocument() {
        final Table table = OracleRelationalMetadataFactory.INSTANCE.tableEditor()
                .tableId(TABLE_ID)
                .addColumn(varcharColumn(8000))
                .addColumn(OracleRelationalMetadataFactory.INSTANCE.columnEditor()
                        .name("ID").type("NUMBER").jdbcType(Types.NUMERIC).length(9).position(2)
                        .create())
                .create();

        final JsonTableChangeSerializer serializer = OracleRelationalMetadataFactory.INSTANCE.jsonTableChangesSerializer();
        assertThat(serializer).isInstanceOf(OracleJsonTableChangeSerializer.class);

        final Document document = serializer.toDocument(table);
        final Document dataColumn = document.getArray("columns").get(0).asDocument();
        assertThat(dataColumn.getDocument(OracleJsonTableChangeSerializer.ORACLE_KEY).getInteger(OracleJsonTableChangeSerializer.OCTET_LENGTH_KEY))
                .isEqualTo(8000);
        assertThat(document.getArray("columns").get(1).asDocument().has(OracleJsonTableChangeSerializer.ORACLE_KEY)).isFalse();

        final Table recovered = serializer.fromDocument(TABLE_ID, document);
        assertThat(recovered).isEqualTo(table);
        assertThat(recovered.columnWithName("DATA").unwrap(OracleColumn.class).octetLength()).isEqualTo(8000);
        assertThat(recovered.columnWithName("ID").unwrap(OracleColumn.class).octetLength()).isEqualTo(Column.UNSET_INT_VALUE);
    }

    @Test
    @FixFor("debezium/dbz#2794")
    void shouldLeaveOctetLengthUnsetWhenReadingLegacySchemaHistoryDocument() {
        final Table table = OracleRelationalMetadataFactory.INSTANCE.tableEditor()
                .tableId(TABLE_ID)
                .addColumn(varcharColumn(8000))
                .create();

        // A document written by the base serializer has no Oracle entry, like history from older versions
        final Document legacy = new JsonTableChangeSerializer().toDocument(table);
        final Table recovered = OracleRelationalMetadataFactory.INSTANCE.jsonTableChangesSerializer().fromDocument(TABLE_ID, legacy);

        final OracleColumn column = recovered.columnWithName("DATA").unwrap(OracleColumn.class);
        assertThat(column.octetLength()).isEqualTo(Column.UNSET_INT_VALUE);
        assertThat(column.length()).isEqualTo(2000);
        assertThat(column.typeName()).isEqualTo("VARCHAR2");
        assertThat(column.jdbcType()).isEqualTo(Types.VARCHAR);
    }

    private static OracleColumn varcharColumn(int octetLength) {
        final OracleColumnEditor editor = OracleRelationalMetadataFactory.INSTANCE.columnEditor();
        // Not optional, because editing an optional column without a default adds an implicit NULL default
        editor.name("DATA").type("VARCHAR2").jdbcType(Types.VARCHAR).length(2000).position(1).optional(false);
        return editor.octetLength(octetLength).create();
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
