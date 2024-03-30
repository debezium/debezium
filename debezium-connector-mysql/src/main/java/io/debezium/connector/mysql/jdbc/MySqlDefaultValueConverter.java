/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.jdbc;

import io.debezium.annotation.Immutable;
import io.debezium.connector.binlog.jdbc.BinlogDefaultValueConverter;

/**
 * This class is used by a DDL parser to convert the string default value to a Java type
 * recognized by value converters for a subset of types. The functionality is kept separate
 * from the main converters to centralize the formatting logic if necessary.
 *
 * @author Jiri Pechanec
 */
@Immutable
public class MySqlDefaultValueConverter extends BinlogDefaultValueConverter {
    public MySqlDefaultValueConverter(MySqlValueConverters valueConverter) {
        super(valueConverter);
    }
}
