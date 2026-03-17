/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.parser.XmlBeginParser;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.text.ParsingException;

/**
 * Unit tests for the Oracle LogMiner {@code XML BEGIN} operation, {@link XmlBeginParser}.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER)
public class XmlBeginParserTest {

    private static final XmlBeginParser parser = new XmlBeginParser();

    @Test
    @FixFor("DBZ-3605")
    public void shouldParseSimpleBinaryXmlBeginRedoSql() {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.XML_TEST"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("DATA").create())
                .create();

        LogMinerEventRow event = binarySqlEvent("XML DOC BEGIN:  select \"DATA\" from \"DEBEZIUM\".\"XML_TEST\" where \"ID\" = '1'");
        final XmlBeginParser.XmlBegin result = parser.parse(event, table);
        assertThat(result.columnName()).isEqualTo("DATA");
        assertThat(result.parsedEvent().getObjectOwner()).isEqualTo("DEBEZIUM");
        assertThat(result.parsedEvent().getObjectName()).isEqualTo("XML_TEST");
    }

    @Test
    @FixFor("dbz#1373")
    public void shouldParseSimpleTextXmlBeginRedoSql() {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.XML_TEST"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("DATA").create())
                .create();

        // update "DEBEZIUM"."DBZ1373" a set a."DATA" = XMLType(:1) where a."ID" = '2';
        LogMinerEventRow event = textSqlEvent("update \"DEBEZIUM\".\"XML_TEST\" a set a.\"DATA\" = XMLType(:1)  where a.\"ID\" = '1';");
        final XmlBeginParser.XmlBegin result = parser.parse(event, table);
        assertThat(result.columnName()).isEqualTo("DATA");
        assertThat(result.parsedEvent().getObjectOwner()).isEqualTo("DEBEZIUM");
        assertThat(result.parsedEvent().getObjectName()).isEqualTo("XML_TEST");
    }

    @Test
    @FixFor("DBZ-3605")
    public void shouldParseSimpleBinaryXmlBeginRedoSqlWithSpacesInObjectNames() {
        final Table table = Table.editor()
                .tableId(TableId.parse("\"DEBEZIUM OBJ\".\"XML_TEST OBJ\""))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("DATA OBJ").create())
                .create();

        LogMinerEventRow event = binarySqlEvent("XML DOC BEGIN:  select \"DATA OBJ\" from \"DEBEZIUM OBJ\".\"XML_TEST OBJ\" where \"ID\" = '1'");
        final XmlBeginParser.XmlBegin result = parser.parse(event, table);
        assertThat(result.columnName()).isEqualTo("DATA OBJ");
        assertThat(result.parsedEvent().getObjectOwner()).isEqualTo("DEBEZIUM OBJ");
        assertThat(result.parsedEvent().getObjectName()).isEqualTo("XML_TEST OBJ");
    }

    @Test
    @FixFor("DBZ-3605")
    public void shouldParseSimpleTextXmlBeginRedoSqlWithSpacesInObjectNames() {
        final Table table = Table.editor()
                .tableId(TableId.parse("\"DEBEZIUM OBJ\".\"XML_TEST OBJ\""))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("DATA OBJ").create())
                .create();

        LogMinerEventRow event = textSqlEvent("update \"DEBEZIUM OBJ\".\"XML_TEST OBJ\" a set a.\"DATA OBJ\" = XMLType(:1)  where a.\"ID\" = '1';");
        final XmlBeginParser.XmlBegin result = parser.parse(event, table);
        assertThat(result.columnName()).isEqualTo("DATA OBJ");
        assertThat(result.parsedEvent().getObjectOwner()).isEqualTo("DEBEZIUM OBJ");
        assertThat(result.parsedEvent().getObjectName()).isEqualTo("XML_TEST OBJ");
    }

    @Test
    @FixFor("DBZ-3605")
    public void shouldNotParseSimpleBinaryXmlBeginRedoSqlWithInvalidPreamble() {
        assertThrows(ParsingException.class, () -> {
            final Table table = Table.editor()
                    .tableId(TableId.parse("DEBEZIUM.XML_TEST"))
                    .addColumn(Column.editor().name("ID").create())
                    .addColumn(Column.editor().name("DATA").create())
                    .create();

            LogMinerEventRow event = binarySqlEvent("XMLDOCBEGIN:  select \"DATA\" from \"DEBEZIUM\".\"XML_TEST\" where \"ID\" = '1'");
            parser.parse(event, table);
        });
    }

    @Test
    @FixFor("DBZ-3605")
    public void shouldNotParseSimpleTextXmlBeginRedoSqlWithInvalidPreamble() {
        assertThrows(ParsingException.class, () -> {
            final Table table = Table.editor()
                    .tableId(TableId.parse("DEBEZIUM.XML_TEST"))
                    .addColumn(Column.editor().name("ID").create())
                    .addColumn(Column.editor().name("DATA").create())
                    .create();

            LogMinerEventRow event = textSqlEvent("updater \"DEBEZIUM\".\"XML_TEST\" a set a.\"DATA\" = XMLType(:1)  where a.\"ID\" = '1';");
            parser.parse(event, table);
        });
    }

    @Test
    @FixFor("DBZ-7489")
    public void shouldParseBinaryXmlDocBeginThatEndsWithIsNull() {
        final Table table = Table.editor()
                .tableId(TableId.parse("SCHEMA.TABLE"))
                .addColumn(Column.editor().name("COLUMN_A").create())
                .addColumn(Column.editor().name("COLUMN_B").create())
                .addColumn(Column.editor().name("COLUMN_D").create())
                .addColumn(Column.editor().name("TIME_A").create())
                .addColumn(Column.editor().name("TIME_B").create())
                .addColumn(Column.editor().name("MODIFICATIONTIME").create())
                .addColumn(Column.editor().name("PROPERTIES").create())
                .create();

        LogMinerEventRow event = binarySqlEvent(
                "XML DOC BEGIN:  select \"PROPERTIES\" from \"SCHEMA\".\"TABLE\" where \"COLUMN_A\" = '314107' and \"COLUMN_B\" = '69265' and \"COLUMN_D\" = '74' and \"TIME_A\" = TO_TIMESTAMP_TZ('2024-02-14 10:58:02.202590 +01:00') and \"TIME_B\" = TO_TIMESTAMP_TZ('3000-01-01 00:00:00.000000 +00:00') and \"MODIFICATIONTIME\" IS NULL");
        parser.parse(event, table);
    }

    @Test
    @FixFor("DBZ-7489")
    public void shouldParseTextXmlDocBeginThatEndsWithIsNull() {
        final Table table = Table.editor()
                .tableId(TableId.parse("SCHEMA.TABLE"))
                .addColumn(Column.editor().name("COLUMN_A").create())
                .addColumn(Column.editor().name("COLUMN_B").create())
                .addColumn(Column.editor().name("COLUMN_D").create())
                .addColumn(Column.editor().name("TIME_A").create())
                .addColumn(Column.editor().name("TIME_B").create())
                .addColumn(Column.editor().name("MODIFICATIONTIME").create())
                .addColumn(Column.editor().name("PROPERTIES").create())
                .create();

        LogMinerEventRow event = textSqlEvent(
                "update \"SCHEMA\".\"TABLE\" a set a.\"PROPERTIES\" = XMLType(:1)  where a.\"COLUMN_A\" = '314107' and a.\"COLUMN_B\" = '69265' and a.\"COLUMN_D\" = '74' and a.\"TIME_A\" = TO_TIMESTAMP_TZ('2024-02-14 10:58:02.202590 +01:00') and a.\"TIME_B\" = TO_TIMESTAMP_TZ('3000-01-01 00:00:00.000000 +00:00') and a.\"MODIFICATIONTIME\" IS NULL;");
        parser.parse(event, table);
    }

    private LogMinerEventRow binarySqlEvent(String sql) {
        return createSqlEvent(sql, "XML sql_redo not re-executable");
    }

    private LogMinerEventRow textSqlEvent(String sql) {
        return createSqlEvent(sql, "XML sql_redo needs assembly");
    }

    private LogMinerEventRow createSqlEvent(String sql, String info) {
        final LogMinerEventRow event = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(event.getRedoSql()).thenReturn(sql);
        Mockito.when(event.getInfo()).thenReturn(info);
        return event;
    }
}
