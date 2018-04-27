/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.Types;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import io.debezium.annotation.Immutable;
import io.debezium.relational.Column;

/**
 * This class is used by a DDL parser to convert the string default value to a Java type
 * recognized by value converters for a subset of types. The functionality is kept separate
 * from the main converters to centralize the formatting logic if necessary.
 *
 * @author Jiri Pechanec
 * @see com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer
 */
@Immutable
public class MySqlDefaultValuePreConverter  {

    /**
     * Converts a default value from the expected format to a logical object acceptable by the main JDBC
     * converter.
     *
     * @param column column definition
     * @param value string formatted default value
     * @return value converted to a Java type
     */
    public Object convert(Column column, String value) {
        if (value == null) {
            return value;
        }
        switch (column.jdbcType()) {
        case Types.DATE:
            return LocalDate.from(DateTimeFormatter.ISO_LOCAL_DATE.parse((String)value));
        case Types.TIMESTAMP:
            return LocalDate.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").parse((String)value));
        }
        return value;
    }
}
