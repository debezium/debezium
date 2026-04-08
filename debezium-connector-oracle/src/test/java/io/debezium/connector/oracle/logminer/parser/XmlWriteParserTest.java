/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;

import oracle.sql.CharacterSet;

/**
 * Unit tests for {@link XmlWriteParser}, specifically verifying that
 * HEXTORAW XML decoding respects the database character set.
 *
 * @author Bjorn Aangbaeck
 */
public class XmlWriteParserTest {

    @Test
    public void shouldDecodeHexToRawXmlWithLatin1CharacterSet() {
        // "<root>Ä</root>" in Latin-1 bytes
        // < = 3c, r = 72, o = 6f, o = 6f, t = 74, > = 3e, Ä = c4, < = 3c, / = 2f, r = 72, o = 6f, o = 6f, t = 74, > = 3e
        final String hexData = "3c726f6f743ec43c2f726f6f743e";
        final String redoSql = "XML_REDO := HEXTORAW('" + hexData + "'):14";

        final LogMinerEventRow event = mockBinaryXmlEvent(redoSql);
        final CharacterSet latin1 = CharacterSet.make(CharacterSet.WE8ISO8859P1_CHARSET);

        final XmlWriteParser.XmlWrite result = XmlWriteParser.parse(event, latin1);

        assertThat(result.data()).isEqualTo("<root>\u00C4</root>");
        assertThat(result.length()).isEqualTo(14);
    }

    @Test
    public void shouldDecodeHexToRawXmlWithUtf8CharacterSet() {
        // "<root>Ä</root>" in UTF-8 bytes
        // Ä in UTF-8 is c3 84
        final String hexData = "3c726f6f743ec3843c2f726f6f743e";
        final String redoSql = "XML_REDO := HEXTORAW('" + hexData + "'):15";

        final LogMinerEventRow event = mockBinaryXmlEvent(redoSql);
        final CharacterSet utf8 = CharacterSet.make(CharacterSet.AL32UTF8_CHARSET);

        final XmlWriteParser.XmlWrite result = XmlWriteParser.parse(event, utf8);

        assertThat(result.data()).isEqualTo("<root>\u00C4</root>");
        assertThat(result.length()).isEqualTo(15);
    }

    @Test
    public void shouldNotCorruptLatin1XmlContent() {
        // "ÅÄÖ" in Latin-1: c5 c4 d6
        final String hexData = "c5c4d6";
        final String redoSql = "XML_REDO := HEXTORAW('" + hexData + "'):3";

        final LogMinerEventRow event = mockBinaryXmlEvent(redoSql);
        final CharacterSet latin1 = CharacterSet.make(CharacterSet.WE8ISO8859P1_CHARSET);

        final XmlWriteParser.XmlWrite result = XmlWriteParser.parse(event, latin1);

        assertThat(result.data()).isEqualTo("\u00C5\u00C4\u00D6");
        assertThat(result.data()).doesNotContain("\uFFFD");
    }

    @Test
    public void shouldHandleInlineXmlWithoutHexToRaw() {
        final String redoSql = "<root>test</root>";

        final LogMinerEventRow event = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(event.getRedoSql()).thenReturn(redoSql);
        Mockito.when(event.getInfo()).thenReturn("");

        final CharacterSet latin1 = CharacterSet.make(CharacterSet.WE8ISO8859P1_CHARSET);

        final XmlWriteParser.XmlWrite result = XmlWriteParser.parse(event, latin1);

        assertThat(result.data()).isEqualTo("<root>test</root>");
    }

    @Test
    public void shouldHandleNullXml() {
        final LogMinerEventRow event = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(event.getRedoSql()).thenReturn("XML_REDO := NULL");

        final CharacterSet latin1 = CharacterSet.make(CharacterSet.WE8ISO8859P1_CHARSET);

        final XmlWriteParser.XmlWrite result = XmlWriteParser.parse(event, latin1);

        assertThat(result.data()).isNull();
        assertThat(result.length()).isEqualTo(0);
    }

    private static LogMinerEventRow mockBinaryXmlEvent(String redoSql) {
        final LogMinerEventRow event = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(event.getRedoSql()).thenReturn(redoSql);
        Mockito.when(event.getInfo()).thenReturn("XML DOC write not re-executable");
        return event;
    }
}
