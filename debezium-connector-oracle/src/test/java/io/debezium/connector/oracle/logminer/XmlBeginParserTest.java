/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
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
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER)
public class XmlBeginParserTest {

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    private static final XmlBeginParser parser = new XmlBeginParser();

    @Test
    @FixFor("DBZ-3605")
    public void shouldParseSimpleXmlBeginRedoSql() throws SQLException {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.XML_TEST"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("DATA").create())
                .create();

        String redoSql = "XML DOC BEGIN:  select \"DATA\" from \"DEBEZIUM\".\"XML_TEST\" where \"ID\" = '1'";
        final LogMinerDmlEntry entry = parser.parse(redoSql, table);
        assertThat(parser.getColumnName()).isEqualTo("DATA");
        assertThat(entry.getObjectOwner()).isEqualTo("DEBEZIUM");
        assertThat(entry.getObjectName()).isEqualTo("XML_TEST");
    }

    @Test
    @FixFor("DBZ-3605")
    public void shouldParseSimpleXmlBeginRedoSqlWithSpacesInObjectNames() throws SQLException {
        final Table table = Table.editor()
                .tableId(TableId.parse("\"DEBEZIUM OBJ\".\"XML_TEST OBJ\""))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("DATA OBJ").create())
                .create();

        String redoSql = "XML DOC BEGIN:  select \"DATA OBJ\" from \"DEBEZIUM OBJ\".\"XML_TEST OBJ\" where \"ID\" = '1'";
        final LogMinerDmlEntry entry = parser.parse(redoSql, table);
        assertThat(parser.getColumnName()).isEqualTo("DATA OBJ");
        assertThat(entry.getObjectOwner()).isEqualTo("DEBEZIUM OBJ");
        assertThat(entry.getObjectName()).isEqualTo("XML_TEST OBJ");
    }

    @Test(expected = ParsingException.class)
    @FixFor("DBZ-3605")
    public void shouldNotParseSimpleXmlBeginRedoSqlWithInvalidPreamble() {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.XML_TEST"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("DATA").create())
                .create();

        String redoSql = "XMLDOCBEGIN:  select \"DATA\" from \"DEBEZIUM\".\"XML_TEST\" where \"ID\" = '1'";
        parser.parse(redoSql, table);
    }

}
