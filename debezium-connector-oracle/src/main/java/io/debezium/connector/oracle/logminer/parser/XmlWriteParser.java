/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import java.nio.charset.StandardCharsets;

import io.debezium.annotation.ThreadSafe;
import io.debezium.text.ParsingException;
import io.debezium.util.Strings;

import oracle.sql.RAW;

/**
 * Parser for decoding an {@code XML_WRITE} LogMiner REDO_SQL value.
 *
 * @author Chris Cranford
 */
@ThreadSafe
public class XmlWriteParser {

    private static final String XML_WRITE_PREAMBLE = "XML_REDO := ";
    private static final String XML_WRITE_PREAMBLE_NULL = XML_WRITE_PREAMBLE + "NULL";

    /**
     * An immutable object representing the parsed data of a {@code XML_WRITE} operation.
     *
     * @param length the length of the data
     * @param data the data
     */
    public record XmlWrite(int length, String data) {
    }

    /**
     * Parses the {@code XML_WRITE} redo SQL
     *
     * @param redoSql the SQL to be parsed
     * @return the parsed details
     */
    public static XmlWrite parse(String redoSql) {
        if (Strings.isNullOrEmpty(redoSql) || !redoSql.startsWith(XML_WRITE_PREAMBLE)) {
            throw new ParsingException(null, "XML write operation does not start with XML_REDO preamble");
        }

        try {
            final String xml;
            if (XML_WRITE_PREAMBLE_NULL.equals(redoSql)) {
                // The XML field is being explicitly set to NULL
                return new XmlWrite(0, null);
            }
            else if (redoSql.charAt(XML_WRITE_PREAMBLE.length()) == '\'') {
                // The XML is not provided as HEXTORAW, which means it was likely stored inline as a
                // VARCHAR column data type because the text is relatively short, i.e. short CLOB.
                int lastQuoteIndex = redoSql.lastIndexOf('\'');
                if (lastQuoteIndex == -1) {
                    throw new IllegalStateException("Failed to find end of XML document");
                }
                // indices here remove leading and trailing single quotes
                xml = redoSql.substring(XML_WRITE_PREAMBLE.length() + 1, lastQuoteIndex);
            }
            else {
                // The XML is provided as HEXTORAW, which means that it was stored out of bands in
                // LOB storage and not inline in the data page. The contents of the XML will
                // require being decoded.
                int lastParenIndex = redoSql.lastIndexOf(')');
                if (lastParenIndex == -1) {
                    throw new IllegalStateException("Failed to find end of XML document");
                }

                // indices are meant to preserve the prefix function call and suffix parenthesis
                String xmlHex = redoSql.substring(XML_WRITE_PREAMBLE.length(), lastParenIndex + 1);

                // NOTE: Oracle generates a small bug here where the initial row starts the function
                // argument with a single quote but the last entry to fulfill the data does not
                // end-quote the argument, but rather simply stops with a parenthesis.
                if (!xmlHex.startsWith("HEXTORAW('") || !xmlHex.endsWith(")")) {
                    throw new IllegalStateException("Invalid HEXTORAW XML decoded data");
                }
                else {
                    if (xmlHex.endsWith("')")) {
                        // Handles situation when Oracle fixes bug
                        xmlHex = xmlHex.substring(10, xmlHex.length() - 2);
                    }
                    else {
                        // Compensates for the bug
                        xmlHex = xmlHex.substring(10, xmlHex.length() - 1);
                    }
                }

                xml = new String(RAW.hexString2Bytes(xmlHex), StandardCharsets.UTF_8);
            }

            int lastColonIndex = redoSql.lastIndexOf(':');
            if (lastColonIndex == -1) {
                throw new IllegalStateException("Failed to find XML document length");
            }

            return new XmlWrite(Integer.parseInt(redoSql.substring(lastColonIndex + 1).trim()), xml);
        }
        catch (Exception e) {
            throw new ParsingException(null, "Failed to parse XML write data", e);
        }
    }
}
