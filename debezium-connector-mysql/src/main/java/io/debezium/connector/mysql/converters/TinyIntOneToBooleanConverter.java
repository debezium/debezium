/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.converters;

/**
 * MySQL reports {@code BOOLEAN} values as {@code TINYINT(1)} in snapshot phase even as a result of
 * {@code DESCRIBE CREATE TABLE}.
 * This custom converter allows user to handle all {@code TINYINT(1)} fields as {@code BOOLEAN} or provide
 * a set of regexes to match only subset of tables/columns.
 *
 * @author Jiri Pechanec
 * @deprecated use {@link io.debezium.connector.binlog.converters.TinyIntOneToBooleanConverter} instead, remove in Debezium 3.0.
 */
@Deprecated
public class TinyIntOneToBooleanConverter extends io.debezium.connector.binlog.converters.TinyIntOneToBooleanConverter {

}
