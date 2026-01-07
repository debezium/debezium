/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.debezium.DebeziumException;
import io.debezium.annotation.ThreadSafe;
import io.debezium.util.Strings;

/**
 * Parser for decoding an {@code LOB_WRITE} LogMiner REDO_SQL value.
 *
 * @author Chris Cranford
 */
@ThreadSafe
public class LobWriteParser {

    private static Pattern SQL_PATTERN = Pattern.compile(
            "(?s).* := ((?:HEXTORAW\\()?'.*'(?:\\))?);\\s*dbms_lob.write\\([^,]+,\\s*(\\d+)\\s*,\\s*(\\d+)\\s*,[^,]+\\);.*");

    /**
     * An immutable object representing the parsed data of a {@code LOB_WRITE} operation.
     *
     * @param offset the write offset
     * @param length the length of the data being written
     * @param data the data
     */
    public record LobWrite(int offset, int length, String data) {
    }

    /**
     * Parses the {@code LOB_WRITE} redo SQL
     *
     * @param redoSql the SQL to be parsed
     * @return the parsed details
     */
    public static LobWrite parse(String redoSql) {
        if (Strings.isNullOrEmpty(redoSql)) {
            return null;
        }

        Matcher m = SQL_PATTERN.matcher(redoSql.trim());
        if (!m.matches()) {
            throw new DebeziumException("Unable to parse unsupported LOB_WRITE SQL: " + redoSql);
        }

        String data = m.group(1);
        if (data.startsWith("'")) {
            // string data; drop the quotes
            data = data.substring(1, data.length() - 1);
        }
        int length = Integer.parseInt(m.group(2));
        int offset = Integer.parseInt(m.group(3)) - 1; // Oracle uses 1-based offsets

        // Double check whether Oracle may have escaped single-quotes in the SQL data.
        // This avoids unintended truncation during the LOB merge phase during the commit
        // logic handled by TransactionCommitConsumer.
        if (data.contains("''")) {
            data = data.replaceAll("''", "'");
        }

        return new LobWrite(offset, length, data);
    }
}
