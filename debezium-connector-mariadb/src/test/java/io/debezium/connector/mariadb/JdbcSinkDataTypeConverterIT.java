/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.connector.binlog.BinlogJdbcSinkDataTypeConverterIT;

/**
 * MariaDB-specific tests with the {@link io.debezium.connector.binlog.converters.JdbcSinkDataTypesConverter}.
 *
 * @author Chris Cranford
 */
public class JdbcSinkDataTypeConverterIT extends BinlogJdbcSinkDataTypeConverterIT<MariaDbConnector> implements MariaDbCommon {

}
